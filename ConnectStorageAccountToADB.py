from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.network.models import (
    PrivateEndpoint,
    PrivateLinkServiceConnection,
    Subnet,
)
import sys
import uuid

# Constants
SUBSCRIPTION_ID = "<>"
RESOURCE_GROUP = "adqueryvnettestrg"
LOCATION = "uksouth"
STORAGE_ACCOUNT_NAME = "adlsstoragedev01"
VNET_NAME = "adbdev2queryvnet2"
PRIVATE_LINK_SUBNET_NAME = "PrivateLink"
PRIVATE_ENDPOINT_NAME = "adls-private-endpoint"
PRIVATE_DNS_ZONE_NAME = "privatelink.dfs.core.windows.net"
WORKSPACE_NAME = "adbworkspacedev01"
ROLE_DEFINITION_NAME = "Storage Blob Data Contributor"

# Azure Clients
credential = DefaultAzureCredential()
resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)
network_client = NetworkManagementClient(credential, SUBSCRIPTION_ID)
storage_client = StorageManagementClient(credential, SUBSCRIPTION_ID)
databricks_client = AzureDatabricksManagementClient(credential, SUBSCRIPTION_ID)
auth_client = AuthorizationManagementClient(credential, SUBSCRIPTION_ID)

# Helper Functions
def get_databricks_managed_identity_principal_id():
    """Retrieve the Managed Identity Principal ID for the Databricks Workspace."""
    print("Retrieving Databricks Managed Identity Principal ID...")
    
    # Fetch the Databricks Workspace details
    workspace = databricks_client.workspaces.get(RESOURCE_GROUP, WORKSPACE_NAME)
    
    # Construct the Managed Resource Group name
    managed_resource_group = workspace.managed_resource_group_id.split("/")[-1]
    print("Printing", managed_resource_group)
    
    # Construct the Managed Identity Resource ID
    managed_identity_resource_id = (
        f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{managed_resource_group}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/dbmanagedidentity"
    )
    
    # Fetch the Managed Identity details
    managed_identity = resource_client.resources.get_by_id(
        managed_identity_resource_id, 
        api_version="2023-01-31"
    )

    # Extract and return the client ID
    try:
        return managed_identity.properties["principalId"]
    except Exception as e:
        raise ValueError("Failed to retrieve the Managed Identity Principal ID.")

def grant_role_to_principal_id(storage_account_id, principal_id, role_definition_name):
    """Grant the specified role to a Principal ID on the storage account."""
    print(f"Granting {role_definition_name} role to Managed Identity Principal ID...")

    # Get the role definition ID
    role_definitions = auth_client.role_definitions.list(
        scope=f"/subscriptions/{SUBSCRIPTION_ID}",
        filter=f"roleName eq '{role_definition_name}'"
    )
    role_definition = next(role_definitions)
    role_definition_id = role_definition.id

    # Assign role
    role_assignment_id = str(uuid.uuid4())  # Unique role assignment ID
    scope = storage_account_id
    try:
        auth_client.role_assignments.create(
            scope,
            role_assignment_id,
            {
                "principal_id": principal_id,
                "role_definition_id": role_definition_id,
                "principal_type": "ServicePrincipal",
            }
        )
        print(f"Role {role_definition_name} assigned successfully to Principal ID {principal_id}.")
    except Exception as e:
        print(f"Failed to assign role: {e}")
        raise

# Main Script
try:
    
    # Get Storage Account Resource ID
    print("Fetching Storage Account Resource ID...")
    storage_account = storage_client.storage_accounts.get_properties(
        RESOURCE_GROUP, STORAGE_ACCOUNT_NAME
    )
    storage_account_id = storage_account.id

    # Fetch Private Link Subnet
    print("Fetching Private Link Subnet...")
    vnet = network_client.virtual_networks.get(RESOURCE_GROUP, VNET_NAME)
    private_link_subnet = next(s for s in vnet.subnets if s.name == PRIVATE_LINK_SUBNET_NAME)

    # Create Private Endpoint
    print("Creating Private Endpoint for ADLS Gen2 Storage Account...")
    private_endpoint = network_client.private_endpoints.begin_create_or_update(
        RESOURCE_GROUP,
        PRIVATE_ENDPOINT_NAME,
        PrivateEndpoint(
            location=LOCATION,
            subnet=Subnet(id=private_link_subnet.id),
            private_link_service_connections=[
                PrivateLinkServiceConnection(
                    name="adls-private-link",
                    private_link_service_id=storage_account_id,
                    group_ids=["dfs"]
                )
            ]
        )
    ).result()
    print(f"Private Endpoint {PRIVATE_ENDPOINT_NAME} created successfully.")
    
    # Create Private DNS Zone
    print(f"Creating Private DNS Zone: {PRIVATE_DNS_ZONE_NAME}...")
    dns_zone_template = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "resources": [
            {
                "type": "Microsoft.Network/privateDnsZones",
                "apiVersion": "2020-06-01",
                "name": PRIVATE_DNS_ZONE_NAME,
                "location": "global",
                "properties": {}
            },
            {
                "type": "Microsoft.Network/privateDnsZones/virtualNetworkLinks",
                "apiVersion": "2020-06-01",
                "name": f"{PRIVATE_DNS_ZONE_NAME}/{PRIVATE_DNS_ZONE_NAME}-link",
                "location": "global",
                "dependsOn": [
                    f"[resourceId('Microsoft.Network/privateDnsZones', '{PRIVATE_DNS_ZONE_NAME}')]"
                ],
                "properties": {
                    "virtualNetwork": {"id": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/{VNET_NAME}"},
                    "registrationEnabled": False
                }
            }
        ]
    }

    dns_deployment = resource_client.deployments.begin_create_or_update(
        RESOURCE_GROUP,
        "PrivateDnsZoneDeployment",
        {
            "properties": {
                "mode": DeploymentMode.INCREMENTAL,
                "template": dns_zone_template,
                "parameters": {}
            }
        }
    ).result()
    print(f"Private DNS Zone {PRIVATE_DNS_ZONE_NAME} and Virtual Network Link created successfully.")

    # Associate DNS Zone with Private Endpoint
    print("Creating DNS Zone Group for Private Endpoint...")
    dns_zone_group_template = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "resources": [
            {
                "type": "Microsoft.Network/privateEndpoints/privateDnsZoneGroups",
                "apiVersion": "2020-03-01",
                "name": f"{PRIVATE_ENDPOINT_NAME}/default",
                "location": "global",
                "properties": {
                    "privateDnsZoneConfigs": [
                        {
                            "name": "dnsZoneConfig",
                            "properties": {
                                "privateDnsZoneId": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/privateDnsZones/{PRIVATE_DNS_ZONE_NAME}"
                            }
                        }
                    ]
                }
            }
        ]
    }

    dns_zone_group_deployment = resource_client.deployments.begin_create_or_update(
        RESOURCE_GROUP,
        "PrivateDnsZoneGroupDeployment",
        {
            "properties": {
                "mode": DeploymentMode.INCREMENTAL,
                "template": dns_zone_group_template,
                "parameters": {}
            }
        }
    ).result()
    print("DNS Zone Group linked to Private Endpoint successfully.")
    

    print("Fetching Storage Account Resource ID...")
    storage_account = storage_client.storage_accounts.get_properties(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME)
    storage_account_id = storage_account.id

    # Get Databricks Managed Identity Client ID
    managed_identity_principal_id = get_databricks_managed_identity_principal_id()

    # Grant "Storage Blob Contributor" Role
    grant_role_to_principal_id(storage_account_id, managed_identity_principal_id, ROLE_DEFINITION_NAME)
    print("Script executed successfully.")

except Exception as e:
    print(f"Error occurred: {e}")
    sys.exit(1)
