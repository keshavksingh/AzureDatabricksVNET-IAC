from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.network.models import (
    VirtualNetwork,
    AddressSpace,
    Subnet,
    NetworkSecurityGroup,
    PrivateEndpoint,
    PrivateLinkServiceConnection,
)
import sys

# Constants
SUBSCRIPTION_ID = "<>"
RESOURCE_GROUP = "adqueryvnettestrg"
LOCATION = "uksouth"
WORKSPACE_NAME = "adbworkspacedev01"
VNET_NAME = "adbdev2queryvnet2"
PUBLIC_SUBNET_NAME = "databricks-source-public-subnet"
PRIVATE_SUBNET_NAME = "databricks-source-private-subnet"
PRIVATE_LINK_SUBNET_NAME = "PrivateLink"
PRIVATE_ENDPOINT_NAME = "adbdevqueryvnet2wsPE"
NSG_NAME = "databricksnsg"
PRIVATE_DNS_ZONE_NAME = "privatelink.azuredatabricks.net"

# Azure Clients
credential = DefaultAzureCredential()
resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)
network_client = NetworkManagementClient(credential, SUBSCRIPTION_ID)
databricks_client = AzureDatabricksManagementClient(credential, SUBSCRIPTION_ID)

# Rollback/Cleanup Function
def rollback_cleanup():
    print("Rolling back: Deleting all resources...")
    try:
        # Delete Databricks Workspace
        print(f"Deleting Databricks Workspace: {WORKSPACE_NAME}")
        databricks_client.workspaces.begin_delete(RESOURCE_GROUP, WORKSPACE_NAME).result()

        # Delete Private Endpoint
        print(f"Deleting Private Endpoint: {PRIVATE_ENDPOINT_NAME}")
        network_client.private_endpoints.begin_delete(RESOURCE_GROUP, PRIVATE_ENDPOINT_NAME).result()

        # Delete Virtual Network
        print(f"Deleting Virtual Network: {VNET_NAME}")
        network_client.virtual_networks.begin_delete(RESOURCE_GROUP, VNET_NAME).result()

        # Delete NSG
        print(f"Deleting Network Security Group: {NSG_NAME}")
        network_client.network_security_groups.begin_delete(RESOURCE_GROUP, NSG_NAME).result()

        # Delete Resource Group
        print(f"Deleting Resource Group: {RESOURCE_GROUP}")
        resource_client.resource_groups.begin_delete(RESOURCE_GROUP).result()

        print("Rollback complete.")
    except Exception as e:
        print(f"Error during rollback: {e}")

# Deployment Process
try:

    # Create Resource Group
    print(f"Creating Resource Group: {RESOURCE_GROUP} in {LOCATION}")
    resource_client.resource_groups.create_or_update(RESOURCE_GROUP, {"location": LOCATION})

    # Create Network Security Group
    print("Creating Network Security Group...")
    nsg = network_client.network_security_groups.begin_create_or_update(
        RESOURCE_GROUP,
        NSG_NAME,
        NetworkSecurityGroup(location=LOCATION)
    ).result()

    # Create Virtual Network and Subnets
    #  subnets with 1024 IP addresses
    print("Creating Virtual Network and Subnets...")
    vnet = network_client.virtual_networks.begin_create_or_update(
        RESOURCE_GROUP,
        VNET_NAME,
        VirtualNetwork(
            location=LOCATION,
            address_space=AddressSpace(address_prefixes=["10.0.0.0/16"]),
            subnets=[
                Subnet(name="default", address_prefix="10.0.0.0/22", network_security_group=nsg),
                Subnet(name=PUBLIC_SUBNET_NAME, address_prefix="10.0.4.0/22", network_security_group=nsg,
                       delegations=[{"name": "databricksDelegation",
                                     "properties": {"serviceName": "Microsoft.Databricks/workspaces"}}]),
                Subnet(name=PRIVATE_SUBNET_NAME, address_prefix="10.0.8.0/22", network_security_group=nsg,
                       delegations=[{"name": "databricksDelegation",
                                     "properties": {"serviceName": "Microsoft.Databricks/workspaces"}}]),
                Subnet(
                    name=PRIVATE_LINK_SUBNET_NAME,
                    address_prefix="10.0.12.0/22",
                    network_security_group=nsg,
                    private_endpoint_network_policies="Disabled",
                    private_link_service_network_policies="Enabled"
                ),
            ]
        )
    ).result()
    print(f"Virtual Network {VNET_NAME} created successfully.")

    # Create Databricks Workspace
    print("Creating Databricks Workspace...")
    workspace = databricks_client.workspaces.begin_create_or_update(
        RESOURCE_GROUP,
        WORKSPACE_NAME,
        {
            "location": LOCATION,
            "sku": {"name": "premium"},
            "properties": {
                "managedResourceGroupId": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/databricks-rg-{WORKSPACE_NAME}",
                "parameters": {
                    "enableNoPublicIp": {"value": True},
                    "customVirtualNetworkId": {"value": vnet.id},
                    "customPublicSubnetName": {"value": PUBLIC_SUBNET_NAME},
                    "customPrivateSubnetName": {"value": PRIVATE_SUBNET_NAME},
                }
            },
            "tags": {"environment": "development", "project": "databricks"}
        }
    ).result()
    print(f"Databricks Workspace {WORKSPACE_NAME} created successfully.")

    # Create Private Endpoint
    print("Creating Private Endpoint for Databricks Workspace...")
    private_endpoint = network_client.private_endpoints.begin_create_or_update(
        RESOURCE_GROUP,
        PRIVATE_ENDPOINT_NAME,
        PrivateEndpoint(
            location=LOCATION,
            subnet=Subnet(id=f"{vnet.id}/subnets/{PRIVATE_LINK_SUBNET_NAME}"),
            private_link_service_connections=[
                PrivateLinkServiceConnection(
                    name=PRIVATE_ENDPOINT_NAME,
                    private_link_service_id=f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Databricks/workspaces/{WORKSPACE_NAME}",
                    group_ids=["databricks_ui_api"]
                )
            ]
        )
    ).result()
    print(f"Private Endpoint {PRIVATE_ENDPOINT_NAME} created successfully.")

    # Deploy ARM Template for Private DNS Zone and Virtual Network Link
    print("Deploying ARM Template for Private DNS Zone and Virtual Network Link...")
    dns_zone_arm_template = {
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
                    "virtualNetwork": {
                        "id": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/{VNET_NAME}"
                    },
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
                "template": dns_zone_arm_template,
                "parameters": {}
            }
        }
    ).result()
    print("Private DNS Zone and Virtual Network Link created successfully.")
    
    # Deploy ARM Template for Private DNS Zone Group
    print("Deploying ARM Template for Private DNS Zone Group...")

    dns_zone_group_arm_template = {
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
                            "name": PRIVATE_DNS_ZONE_NAME,
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
                "template": dns_zone_group_arm_template,
                "parameters": {}
            }
        }
    ).result()
    print("Private DNS Zone Group associated with Private Endpoint successfully.")

except Exception as e:
    print(f"Error occurred: {e}")
    rollback_cleanup()
    sys.exit(1)
