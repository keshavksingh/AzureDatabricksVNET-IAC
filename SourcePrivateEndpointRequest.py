from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.identity import DefaultAzureCredential
from azure.mgmt.network.models import (
    VirtualNetwork,
    AddressSpace,
    Subnet,
    NetworkSecurityGroup,
    PrivateEndpoint,
    PrivateLinkServiceConnection,
)

# Initialize Azure clients
SUBSCRIPTION_ID = "<>"
RESOURCE_GROUP = "adqueryvnettestrg"
WORKSPACE_NAME = "adbdevsourcews"
VNET_NAME = "adbdev2queryvnet2"
PRIVATE_LINK_SUBNET_NAME = "PrivateLink"

LOCATION = "uksouth"
PRIVATE_ENDPOINT_NAME = "adbPrivateEndpointCustomerWorkspace"
DATABRICKS_WORKSPACE_RESOURCE_ID = f"/subscriptions/27ef0436-f648-4bad-be15-3e872e16318b/resourceGroups/adbsourcevnetrg/providers/Microsoft.Databricks/workspaces/adbdevsourcews"
VNET_ID = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/{VNET_NAME}"
SUBNET_ID = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/{VNET_NAME}/subnets/{PRIVATE_LINK_SUBNET_NAME}"
PRIVATE_DNS_ZONE_NAME = "privatelink.azuredatabricks.net"
PRIVATE_DNS_ZONE_RESOURCE_ID = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/privateDnsZones/privatelink.azuredatabricks.net"

credential = DefaultAzureCredential()
network_client = NetworkManagementClient(credential, SUBSCRIPTION_ID)
resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)

print("Creating Private Endpoint...")
private_endpoint = network_client.private_endpoints.begin_create_or_update(
    RESOURCE_GROUP,
    PRIVATE_ENDPOINT_NAME,
    PrivateEndpoint(
        location=LOCATION,
        subnet=Subnet(id=SUBNET_ID),
        private_link_service_connections=[
            PrivateLinkServiceConnection(
                name=PRIVATE_ENDPOINT_NAME,
                private_link_service_id=DATABRICKS_WORKSPACE_RESOURCE_ID,
                group_ids=["databricks_ui_api"]
            )
        ]
    )
).result()
print(f"Private Endpoint '{PRIVATE_ENDPOINT_NAME}' created successfully.")

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

