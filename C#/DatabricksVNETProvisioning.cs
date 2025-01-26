using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Resources.Models;
using Azure.ResourceManager.Network;
using Azure.ResourceManager.Network.Models;

using Azure.Core;

namespace VNET_IAC
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await DatabricksVNETProvisioning.Run(args);
        }
    }

    class DatabricksVNETProvisioning
    {
        // Constants
        private static string SUBSCRIPTION_ID = "<subsId>;
        private static string RESOURCE_GROUP = "adcsqueryvnetrg";
        private static string LOCATION = "uksouth";
        private static string WORKSPACE_NAME = "adbcsworkspacedev01";
        private static string VNET_NAME = "adbcsdevqueryvnet";
        private static string VNET_ID = $"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/{VNET_NAME}";
        private static string PUBLIC_SUBNET_NAME = "databricks-public-subnet";
        private static string PRIVATE_SUBNET_NAME = "databricks-private-subnet";
        private static string PRIVATE_LINK_SUBNET_NAME = "PrivateLink";
        
        private static string PRIVATE_ENDPOINT_NAME = "adbcsdevqueryvnetPE";
        private static string NSG_NAME = "databricksnsg";
        private static string PRIVATE_DNS_ZONE_NAME = "privatelink.azuredatabricks.net";

        public static async Task Run(string[] args)
        {
            try
            {
                // Authenticate and initialize Azure clients
                var credential = new DefaultAzureCredential();
                var armClient = new ArmClient(credential);
                var subscription = armClient.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{SUBSCRIPTION_ID}"));
                var resourceGroup = await CreateResourceGroup(subscription);
                var networkClient = subscription.GetNetworkSecurityGroups();

                // Create Network Security Group
                Console.WriteLine("Creating Network Security Group...");
                var nsg = await CreateNetworkSecurityGroup(resourceGroup);

                // Create Virtual Network and Subnets
                Console.WriteLine("Creating Virtual Network and Subnets...");
                var vnet = await CreateVirtualNetwork(resourceGroup, nsg);

                // Create Databricks Workspace
                Console.WriteLine("Creating Databricks Workspace...");
                var workspace = await DeployDatabricksWorkspaceUsingArmTemplate(resourceGroup, vnet);

                // Create Private Endpoint
                Console.WriteLine("Creating Private Endpoint for Databricks Workspace...");
                var privateEndpoint = await CreatePrivateEndpoint(resourceGroup, vnet, SUBSCRIPTION_ID, RESOURCE_GROUP, workspace);
                
                // Deploy ARM Templates for DNS and Links
                Console.WriteLine("Deploying ARM Template for Private DNS Zone and Virtual Network Link...");
                await DeployDnsZoneAndLink(resourceGroup);

                Console.WriteLine("Deploying ARM Template for Private DNS Zone Group...");
                await DeployDnsZoneGroup(resourceGroup);

                Console.WriteLine("Provisioning completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred: {ex.Message}");
                await RollbackCleanup();
            }
        }

        private static async Task<ResourceGroupResource> CreateResourceGroup(SubscriptionResource subscription)
        {
            Console.WriteLine($"Creating Resource Group: {RESOURCE_GROUP} in {LOCATION}");
            var resourceGroupData = new ResourceGroupData(LOCATION);
            var operation = await subscription.GetResourceGroups().CreateOrUpdateAsync(WaitUntil.Completed, RESOURCE_GROUP, resourceGroupData);
            return operation.Value;
        }

        private static async Task<NetworkSecurityGroupResource> CreateNetworkSecurityGroup(ResourceGroupResource resourceGroup)
        {
            var nsgData = new NetworkSecurityGroupData { Location = LOCATION };
            var operation = await resourceGroup.GetNetworkSecurityGroups().CreateOrUpdateAsync(
                WaitUntil.Completed,
                NSG_NAME,
                nsgData);
            return operation.Value;
        }

        private static async Task<VirtualNetworkResource> CreateVirtualNetwork(ResourceGroupResource resourceGroup, NetworkSecurityGroupResource nsg)
        {
            var vnetData = new VirtualNetworkData
            {
                Location = LOCATION,
                AddressSpace = new VirtualNetworkAddressSpace { AddressPrefixes = { "10.0.0.0/16" } },
                Subnets =
        {
            new SubnetData
            {
                Name = "default",
                AddressPrefix = "10.0.0.0/22",
                NetworkSecurityGroup = nsg.Data
            },
            new SubnetData
            {
                Name = PUBLIC_SUBNET_NAME,
                AddressPrefix = "10.0.4.0/22",
                NetworkSecurityGroup = nsg.Data,
                Delegations =
                {
                    new ServiceDelegation
                    {
                        Name = "databricksDelegation",
                        ServiceName = "Microsoft.Databricks/workspaces"
                    }
                }
            },
            new SubnetData
            {
                Name = PRIVATE_SUBNET_NAME,
                AddressPrefix = "10.0.8.0/22",
                NetworkSecurityGroup = nsg.Data,
                Delegations =
                {
                    new ServiceDelegation
                    {
                        Name = "databricksDelegation",
                        ServiceName = "Microsoft.Databricks/workspaces"
                    }
                }
            },
            new SubnetData
            {
                Name = PRIVATE_LINK_SUBNET_NAME,
                AddressPrefix = "10.0.12.0/22",
                NetworkSecurityGroup = nsg.Data,
                PrivateEndpointNetworkPolicy = VirtualNetworkPrivateEndpointNetworkPolicy.Disabled,
                PrivateLinkServiceNetworkPolicy = VirtualNetworkPrivateLinkServiceNetworkPolicy.Enabled
            }
        }
            };
            var operation = await resourceGroup.GetVirtualNetworks().CreateOrUpdateAsync(WaitUntil.Completed, VNET_NAME, vnetData);
            return operation.Value;
        }

        private static async Task<string> DeployDatabricksWorkspaceUsingArmTemplate(ResourceGroupResource resourceGroup, VirtualNetworkResource vnet)
        {
            Console.WriteLine("Deploying Databricks Workspace using ARM Template...");

            // Define the ARM template JSON as a string
            string armTemplateJson = @"
{
    ""$schema"": ""https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#"",
    ""contentVersion"": ""1.0.0.0"",
    ""resources"": [
        {
            ""type"": ""Microsoft.Databricks/workspaces"",
            ""apiVersion"": ""2024-09-01-preview"",
            ""name"": ""[parameters('workspaceName')]"",
            ""location"": ""[parameters('location')]"",
            ""sku"": {
                ""name"": ""premium""
            },
            ""properties"": {
                ""managedResourceGroupId"": ""[concat('/subscriptions/', subscription().subscriptionId, '/resourceGroups/databricks-rg-', parameters('workspaceName'))]"",
                ""parameters"": {
                    ""enableNoPublicIp"": { ""value"": ""[parameters('enableNoPublicIp')]"" },
                    ""customVirtualNetworkId"": { ""value"": ""[parameters('customVirtualNetworkId')]"" },
                    ""customPublicSubnetName"": { ""value"": ""[parameters('customPublicSubnetName')]"" },
                    ""customPrivateSubnetName"": { ""value"": ""[parameters('customPrivateSubnetName')]"" }
                }
            },
            ""tags"": {
                ""environment"": ""vnet"",
                ""project"": ""databricks""
            }
        }
    ],
    ""parameters"": {
        ""workspaceName"": { ""type"": ""string"" },
        ""location"": { ""type"": ""string"" },
        ""enableNoPublicIp"": { ""type"": ""bool"" },
        ""customVirtualNetworkId"": { ""type"": ""string"" },
        ""customPublicSubnetName"": { ""type"": ""string"" },
        ""customPrivateSubnetName"": { ""type"": ""string"" }
    }
}";

            // Define the parameters for the ARM template
            var parameters = new
            {
                workspaceName = new { value = WORKSPACE_NAME },
                location = new { value = LOCATION },
                enableNoPublicIp = new { value = true },
                customVirtualNetworkId = new { value = VNET_ID },
                customPublicSubnetName = new { value = PUBLIC_SUBNET_NAME },
                customPrivateSubnetName = new { value = PRIVATE_SUBNET_NAME }
            };

            // Convert the ARM template JSON to BinaryData
            var armTemplateBinaryData = BinaryData.FromString(armTemplateJson);

            // Convert parameters to BinaryData
            var parametersBinaryData = BinaryData.FromObjectAsJson(parameters);

            // Create the deployment object
            var deployment = new ArmDeploymentContent(new ArmDeploymentProperties(ArmDeploymentMode.Incremental)
            {
                Template = armTemplateBinaryData,
                Parameters = parametersBinaryData
            });

            var deploymentName = $"databricks-deployment-{Guid.NewGuid()}";

            try
            {
                // Deploy the ARM template
                var deploymentOperation = await resourceGroup.GetArmDeployments().CreateOrUpdateAsync(WaitUntil.Completed, deploymentName, deployment);

                Console.WriteLine($"Databricks Workspace deployment {deploymentName} completed successfully.");
                return WORKSPACE_NAME;
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Deployment failed with error: {ex.Message}");

                // Log deployment operations
                var deploymentResource = await resourceGroup.GetArmDeployments().GetAsync(deploymentName);
                var operations = deploymentResource.Value.GetDeploymentOperations();
                foreach (var operation in operations)
                {
                    Console.WriteLine($"Operation: {operation.Id}, Status: {operation.Properties.ProvisioningState}");
                    if (operation.Properties.StatusMessage != null)
                    {
                        Console.WriteLine($"Error: {operation.Properties.StatusMessage}");
                    }
                }

                throw;
            }
        }

        private static async Task<PrivateEndpointResource> CreatePrivateEndpoint(ResourceGroupResource resourceGroup, VirtualNetworkResource vnet, string SUBSCRIPTION_ID, string RESOURCE_GROUP, string WORKSPACE_NAME)
        {
            var privateEndpointData = new PrivateEndpointData
            {
                Location = LOCATION,
                Subnet = new SubnetData { Id = new ResourceIdentifier($"{vnet.Data.Id}/subnets/{PRIVATE_LINK_SUBNET_NAME}") },
                PrivateLinkServiceConnections =
        {
            new NetworkPrivateLinkServiceConnection
            {
                Name = PRIVATE_ENDPOINT_NAME,
                PrivateLinkServiceId = new ResourceIdentifier($"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Databricks/workspaces/{WORKSPACE_NAME}"),
                GroupIds = { "databricks_ui_api" }
            }
        }
            };
            var operation = await resourceGroup.GetPrivateEndpoints().CreateOrUpdateAsync(WaitUntil.Completed, PRIVATE_ENDPOINT_NAME, privateEndpointData);
            return operation.Value;
        }

        private static async Task DeployDnsZoneAndLink(ResourceGroupResource resourceGroup)
        {
            Console.WriteLine("Deploying Private DNS Zone and Virtual Network Link using ARM Template...");

            // Define the ARM template JSON as a string
            string armTemplateJson = @"
{
    ""$schema"": ""https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"",
    ""contentVersion"": ""1.0.0.0"",
    ""resources"": [
        {
            ""type"": ""Microsoft.Network/privateDnsZones"",
            ""apiVersion"": ""2020-06-01"",
            ""name"": ""[parameters('dnsZoneName')]"",
            ""location"": ""global"",
            ""properties"": {}
        },
        {
            ""type"": ""Microsoft.Network/privateDnsZones/virtualNetworkLinks"",
            ""apiVersion"": ""2020-06-01"",
            ""name"": ""[concat(parameters('dnsZoneName'), '/', parameters('dnsZoneLinkName'))]"",
            ""location"": ""global"",
            ""dependsOn"": [
                ""[resourceId('Microsoft.Network/privateDnsZones', parameters('dnsZoneName'))]""
            ],
            ""properties"": {
                ""virtualNetwork"": {
                    ""id"": ""[parameters('virtualNetworkId')]""
                },
                ""registrationEnabled"": false
            }
        }
    ],
    ""parameters"": {
        ""dnsZoneName"": {
            ""type"": ""string""
        },
        ""dnsZoneLinkName"": {
            ""type"": ""string""
        },
        ""virtualNetworkId"": {
            ""type"": ""string""
        }
    }
}";

            var parameters = new
            {
                dnsZoneName = new { value = PRIVATE_DNS_ZONE_NAME },
                dnsZoneLinkName = new { value = $"{PRIVATE_DNS_ZONE_NAME}-link" },
                virtualNetworkId = new { value = VNET_ID }
            };

            var armTemplateBinaryData = BinaryData.FromString(armTemplateJson);
            var parametersBinaryData = BinaryData.FromObjectAsJson(parameters);
            var deployment = new ArmDeploymentContent(new ArmDeploymentProperties(ArmDeploymentMode.Incremental)
            {
                Template = armTemplateBinaryData,
                Parameters = parametersBinaryData
            });

            var deploymentName = $"dns-zone-deployment-{Guid.NewGuid()}";

            try
            {
                var deploymentOperation = await resourceGroup.GetArmDeployments().CreateOrUpdateAsync(WaitUntil.Completed, deploymentName, deployment);

                Console.WriteLine($"Private DNS Zone and Virtual Network Link deployment {deploymentName} completed successfully.");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Deployment failed with error: {ex.Message}");
                var deploymentResource = await resourceGroup.GetArmDeployments().GetAsync(deploymentName);
                var operations = deploymentResource.Value.GetDeploymentOperations();
                foreach (var operation in operations)
                {
                    Console.WriteLine($"Operation: {operation.Id}, Status: {operation.Properties.ProvisioningState}");
                    if (operation.Properties.StatusMessage != null)
                    {
                        Console.WriteLine($"Error: {operation.Properties.StatusMessage}");
                    }
                }

                throw;
            }
        }

        private static async Task DeployDnsZoneGroup(ResourceGroupResource resourceGroup)
        {
            Console.WriteLine("Deploying ARM Template for Private DNS Zone Group associated with Private Endpoint...");

            // Define the ARM template JSON as a string
            string armTemplateJson = @"
{
    ""$schema"": ""https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"",
    ""contentVersion"": ""1.0.0.0"",
    ""resources"": [
        {
            ""type"": ""Microsoft.Network/privateEndpoints/privateDnsZoneGroups"",
            ""apiVersion"": ""2020-03-01"",
            ""name"": ""[concat(parameters('privateEndpointName'), '/default')]"",
            ""location"": ""global"",
            ""properties"": {
                ""privateDnsZoneConfigs"": [
                    {
                        ""name"": ""[parameters('privateDnsZoneName')]"",
                        ""properties"": {
                            ""privateDnsZoneId"": ""[concat('/subscriptions/', subscription().subscriptionId, '/resourceGroups/', parameters('resourceGroupName'), '/providers/Microsoft.Network/privateDnsZones/', parameters('privateDnsZoneName'))]""
                        }
                    }
                ]
            }
        }
    ],
    ""parameters"": {
        ""privateEndpointName"": {
            ""type"": ""string""
        },
        ""resourceGroupName"": {
            ""type"": ""string""
        },
        ""privateDnsZoneName"": {
            ""type"": ""string""
        }
    }
}";
            var parameters = new
            {
                privateEndpointName = new { value = PRIVATE_ENDPOINT_NAME },
                resourceGroupName = new { value = RESOURCE_GROUP },
                privateDnsZoneName = new { value = PRIVATE_DNS_ZONE_NAME }
            };

            var armTemplateBinaryData = BinaryData.FromString(armTemplateJson);
            var parametersBinaryData = BinaryData.FromObjectAsJson(parameters);
            var deployment = new ArmDeploymentContent(new ArmDeploymentProperties(ArmDeploymentMode.Incremental)
            {
                Template = armTemplateBinaryData,
                Parameters = parametersBinaryData
            });

            var deploymentName = $"dns-zone-group-deployment-{Guid.NewGuid()}";

            try
            {
                var deploymentOperation = await resourceGroup.GetArmDeployments().CreateOrUpdateAsync(WaitUntil.Completed, deploymentName, deployment);
                Console.WriteLine($"Private DNS Zone Group associated with Private Endpoint successfully deployed: {deploymentName}");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Deployment failed with error: {ex.Message}");
                var deploymentResource = await resourceGroup.GetArmDeployments().GetAsync(deploymentName);
                var operations = deploymentResource.Value.GetDeploymentOperations();
                foreach (var operation in operations)
                {
                    Console.WriteLine($"Operation: {operation.Id}, Status: {operation.Properties.ProvisioningState}");
                    if (operation.Properties.StatusMessage != null)
                    {
                        Console.WriteLine($"Error: {operation.Properties.StatusMessage}");
                    }
                }
                throw;
            }
        }

        private static async Task RollbackCleanup()
        {
            Console.WriteLine("Rolling back: Deleting all resources...");

            try
            {
                // Authenticate and initialize Azure clients
                var credential = new DefaultAzureCredential();
                var armClient = new ArmClient(credential);
                var subscription = armClient.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{SUBSCRIPTION_ID}"));
                var resourceGroup = await subscription.GetResourceGroupAsync(RESOURCE_GROUP);

                // Delete Resource Group
                try
                {
                    Console.WriteLine($"Deleting Resource Group: {RESOURCE_GROUP}");
                    await resourceGroup.Value.DeleteAsync(WaitUntil.Completed);
                    Console.WriteLine($"Resource Group {RESOURCE_GROUP} deleted.");
                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"Error deleting Resource Group: {ex.Message}");
                }

                Console.WriteLine("Rollback completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during rollback: {ex.Message}");
            }
        }


    }

}
