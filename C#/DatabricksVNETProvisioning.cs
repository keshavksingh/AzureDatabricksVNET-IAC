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
using Azure.ResourceManager.Storage;
using Microsoft.Azure.Databricks.Client;
using Microsoft.Azure.Databricks.Client.Models;
using Azure.ResourceManager.Authorization;
using Azure.ResourceManager.Authorization.Models;
using Azure.Core;

namespace VNET_IAC
{
    class Program
    {
        static async Task Main(string[] args)
        {
            
            await DatabricksVNETProvisioning.Run(args);
            var databricksHelper = new DatabricksHelper("<SubsId>", "adcsqueryvnetrg", "adbcsworkspacedev01");
            string principalId = await databricksHelper.GetDatabricksManagedIdentityPrincipalIdAsync();
            Guid principalGuid = Guid.Parse(principalId);
            Console.WriteLine($"Managed Identity Principal ID: {principalId}");

            string subscriptionId = "<SubsId>";
            string resourceGroup = "adcsqueryvnetrg";
            string storageAccountName = "<ADLSGen2Name>";
            string storageAccountScope = $"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.Storage/storageAccounts/{storageAccountName}";
            var roleHelper = new RoleAssignmentHelper(subscriptionId);
            try
            {
                await roleHelper.AssignRoleToServicePrincipalAsync(principalGuid, storageAccountScope);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            string databricksWorkspaceUrl = "https://adb-<>.<>.azuredatabricks.net";
            string aadToken = DatabricksClusterHelper.GetAzureADToken();
            var databricksHelper = new DatabricksClusterHelper(databricksWorkspaceUrl, aadToken);
            string clusterId = await databricksHelper.CreateClusterAsync();
            Console.WriteLine($"Cluster successfully created with ID: {clusterId}");
        }
    }

    class DatabricksVNETProvisioning
    {
        // Constants
        private static string SUBSCRIPTION_ID = "<>";
        private static string RESOURCE_GROUP = "<ResourceGroupName>";
        private static string LOCATION = "uksouth";
        private static string WORKSPACE_NAME = "<ADBWSName>";
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

    public class DatabricksHelper
    {
        private static string SUBSCRIPTION_ID = "<>";
        private static string RESOURCE_GROUP = "<>";
        private static string WORKSPACE_NAME = "<>";
        private readonly ArmClient _armClient;

        public DatabricksHelper(string SUBSCRIPTION_ID, string RESOURCE_GROUP, string WORKSPACE_NAME)
        {
        // Initialize the Azure ARM Client using DefaultAzureCredential
        _armClient = new ArmClient(new DefaultAzureCredential(), SUBSCRIPTION_ID);
        }

        public async Task<string> GetDatabricksManagedIdentityPrincipalIdAsync()
        {
            try
            {
                Console.WriteLine("Retrieving Databricks Managed Identity Principal ID...");
                var resourceGroup = _armClient.GetResourceGroupResource(
                    new ResourceIdentifier($"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}")
                );

                string workspaceResourceId = $"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Databricks/workspaces/{WORKSPACE_NAME}";
                var workspaceResource = await _armClient.GetGenericResource(new ResourceIdentifier(workspaceResourceId)).GetAsync();

                var workspaceProperties = workspaceResource.Value.Data.Properties.ToObjectFromJson<Dictionary<string, object>>();
                if (workspaceProperties.TryGetValue("managedResourceGroupId", out var managedResourceGroupId))
                {
                    string managedResourceGroupName = managedResourceGroupId.ToString().Split('/').Last();
                    Console.WriteLine($"Managed Resource Group Name: {managedResourceGroupName}");
                    var managedIdentityResourceId = $"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{managedResourceGroupName}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/dbmanagedidentity";
                    var managedIdentityResource = await _armClient.GetGenericResource(new ResourceIdentifier(managedIdentityResourceId)).GetAsync();
                    var managedIdentityProperties = managedIdentityResource.Value.Data.Properties.ToObjectFromJson<Dictionary<string, object>>();
                    if (managedIdentityProperties != null && managedIdentityProperties.TryGetValue("principalId", out var principalId))
                    {
                        return principalId?.ToString() ?? throw new InvalidOperationException("Principal ID is null.");
                    }
                    else
                    {
                        throw new InvalidOperationException("Failed to retrieve the Managed Identity Principal ID.");
                    }
                }
                else
                {
                    Console.WriteLine("managedResourceGroupId not found in workspace properties.");
                    return null;
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Azure Request Error: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"General Error: {ex.Message}");
                throw new InvalidOperationException("An error occurred while retrieving the Managed Identity Principal ID.", ex);
            }
        }



    }

    public class RoleAssignmentHelper
    {
        private readonly ArmClient _armClient;
        private readonly string _subscriptionId;

        // Predefined Role Definition ID for "Storage Blob Data Contributor"
        private const string StorageBlobDataContributorRoleId = "ba92f5b4-2d11-453d-a403-e96b0029c9fe";

        public RoleAssignmentHelper(string subscriptionId)
        {
            _subscriptionId = subscriptionId;
            _armClient = new ArmClient(new DefaultAzureCredential(), _subscriptionId);
        }
        public async Task AssignRoleToServicePrincipalAsync(Guid principalId, string scopePath, string roleDefinitionId = StorageBlobDataContributorRoleId)
        {
            try
            {
                var roleName = StorageBlobDataContributorRoleId;
                Console.WriteLine($"Assigning role '{roleDefinitionId}' to principal '{principalId}' on scope '{scopePath}'...");
                var roleDefId = $"/subscriptions/{_subscriptionId}/providers/Microsoft.Authorization/roleDefinitions/{roleName}";
                var roleAssignmentResourceId = RoleAssignmentResource.CreateResourceIdentifier(scopePath, Guid.NewGuid().ToString());
                var roleAssignmentResource = _armClient.GetRoleAssignmentResource(roleAssignmentResourceId);

                var roleAssignmentContent = new RoleAssignmentCreateOrUpdateContent(new ResourceIdentifier(roleDefId), principalId)
                {
                    PrincipalType = RoleManagementPrincipalType.ServicePrincipal
                };
                
                await roleAssignmentResource.UpdateAsync(WaitUntil.Completed, roleAssignmentContent);

                Console.WriteLine($"Successfully assigned role to principal '{principalId}' on scope '{scopePath}'.");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Azure request failed: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
                throw;
            }
        }
    }

    public class DatabricksClusterHelper
    {
        private readonly DatabricksClient _databricksClient;

        // Constructor initializes the Databricks Client
        public DatabricksClusterHelper(string databricksWorkspaceUrl, string aadAccessToken)
        {
            _databricksClient = DatabricksClient.CreateClient(databricksWorkspaceUrl, aadAccessToken);
        }

        // Method to create a Databricks cluster
        public async Task<string> CreateClusterAsync()
        {
            Console.WriteLine("Creating Databricks cluster...");

            // Cluster configuration
            var clusterConfig = ClusterAttributes
                .GetNewClusterConfiguration("StandardCluster")
                .WithRuntimeVersion("16.1.x-scala2.12")
                .WithAutoScale(2, 4)
                .WithAutoTermination(30)
                .WithNodeType("Standard_D4ds_v5")
                .WithClusterMode(ClusterMode.Standard);

            // Set Spark configuration using the property instead of method
            clusterConfig.SparkConfiguration = new Dictionary<string, string>{{ "spark.databricks.delta.preview.enabled", "true" }};
            var libraries = new List<Library> { new MavenLibrary { MavenLibrarySpec = new MavenLibrarySpec { Coordinates = "com.databricks:databricks-jdbc:2.6.40" } } };

            try
            {
                // Create the cluster
                var clusterId = await _databricksClient.Clusters.Create(clusterConfig);
                Console.WriteLine("Cluster created successfully!");
                Console.WriteLine($"Cluster ID: {clusterId}");
                return clusterId;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to create cluster.");
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }

        // Method to get the Azure AD token (if needed separately)
        public static string GetAzureADToken()
        {
            var credential = new Azure.Identity.DefaultAzureCredential();
            var tokenRequestContext = new Azure.Core.TokenRequestContext(new[] { "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default" });
            var token = credential.GetToken(tokenRequestContext);
            return token.Token;
        }
    }


}
