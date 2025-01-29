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
using Newtonsoft.Json;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace VNET_IAC
{
    class Program
    {
        static async Task Main(string[] args)
        {
            
            await DatabricksVNETProvisioning.Run(args);
            var databricksHelper = new DatabricksHelper("<SUBS_ID>", "<RG>", "<ADB_WS_NAME>");
            (string principalId, string clientId) = await databricksHelper.GetDatabricksManagedIdentityPrincipalIdAsync();
            Guid principalGuid = Guid.Parse(principalId);
            Console.WriteLine($"Managed Identity Principal ID: {principalId}");
            Guid clientIdGuid = Guid.Parse(clientId);
            Console.WriteLine($"Managed Identity Client ID: {clientId}");

            string subscriptionId = "<SUBS_ID>";
            string resourceGroup = "<RG>";
            string storageAccountName = "shared_adlsgen2_Name";
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
            
            string TenantId = "<TENANT_ID>";
            string databricksWorkspaceUrl = "https://adb-<>.<>.azuredatabricks.net";
            string aadToken = DatabricksClusterHelper.GetAzureADToken();
            var databricksClusterHelper = new DatabricksClusterHelper(databricksWorkspaceUrl, aadToken);
            string clusterId = await databricksClusterHelper.CreateClusterAsync(storageAccountName, clientId, TenantId);
            Console.WriteLine($"Cluster successfully created with ID: {clusterId}");

            //Create Databricks Job
            string databricksToken = aadToken;
            string jarPath = "abfss://jarcontainer@<storageaccount>.dfs.core.windows.net/jardir/<jar>-assembly-1.1.jar";
            string mainClassName = "com.microsoft.<>.<>.MainApp";

            DatabricksCreateJobHelper jobHelper = new DatabricksCreateJobHelper(databricksToken, databricksWorkspaceUrl);
            string jobId = await jobHelper.CreateJobAsync(clusterId, jarPath, mainClassName);

            if (!string.IsNullOrEmpty(jobId))
            {
                Console.WriteLine($"Job created successfully with ID: {jobId}");
            }
            else
            {
                Console.WriteLine("Failed to create job.");
            }

            //string jobId = "296921761618670";
            var parameters = new Dictionary<string, string>
                                {
                                    { "param1", "value1" },
                                    { "param2", "value2" }
                                };
            
            var runJobHelper = new DatabricksRunJobHelper(databricksToken, databricksWorkspaceUrl);
            string? runId = await runJobHelper.RunJobAsync(jobId, parameters);

            if (!string.IsNullOrEmpty(runId))
            {
                Console.WriteLine($"Job started successfully with Run ID: {runId}");
            }
            else
            {
                Console.WriteLine("Job run failed.");
            }
            
            string jobRunId = "844368691789134"; // Replace with actual job run ID

            var jobTaskHelper = new DatabricksJobTaskHelper(databricksToken, databricksWorkspaceUrl);
            var output = await jobTaskHelper.GetJobTaskOutputAsync(jobRunId);

            if (output != null)
            {
                Console.WriteLine("Job task output retrieved successfully.");
            }
            else
            {
                Console.WriteLine("Failed to retrieve job task output.");
            }
        }
    }

    class DatabricksVNETProvisioning
    {
        // Constants
        private static string SUBSCRIPTION_ID = "";
        private static string RESOURCE_GROUP = "";
        private static string LOCATION = "uksouth";
        private static string WORKSPACE_NAME = "";
        private static string VNET_NAME = "";
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
        private static string SUBSCRIPTION_ID = "27ef0436-f648-4bad-be15-3e872e16318b";
        private static string RESOURCE_GROUP = "adcsqueryvnetrg";
        private static string WORKSPACE_NAME = "adbcsworkspacedev01";
        private readonly ArmClient _armClient;

        public DatabricksHelper(string SUBSCRIPTION_ID, string RESOURCE_GROUP, string WORKSPACE_NAME)
        {
        // Initialize the Azure ARM Client using DefaultAzureCredential
        _armClient = new ArmClient(new DefaultAzureCredential(), SUBSCRIPTION_ID);
        }

        public async Task<(string,string)> GetDatabricksManagedIdentityPrincipalIdAsync()
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
                    if (managedIdentityProperties != null && managedIdentityProperties.TryGetValue("principalId", out var principalId) && managedIdentityProperties.TryGetValue("clientId", out var clientId))
                    {
                        var principalIdStr = principalId?.ToString() ?? throw new InvalidOperationException("Principal ID is null.");
                        var clientIdStr = clientId?.ToString() ?? throw new InvalidOperationException("Client ID is null.");

                        return (principalIdStr, clientIdStr);
                    }
                    else
                    {
                        throw new InvalidOperationException("Failed to retrieve the Managed Identity Principal ID.");
                    }
                }
                else
                {
                    Console.WriteLine("managedResourceGroupId not found in workspace properties.");
                    return (string.Empty, string.Empty);
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
        public async Task<string> CreateClusterAsync(string StorageAccount, string ClientId, string TenantId)
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
            clusterConfig.SparkConfiguration = new Dictionary<string, string>{{ "spark.hadoop.fs.azure.account.oauth2.client.id", $"{ClientId}" }
                ,{ $"spark.hadoop.fs.azure.account.oauth.provider.type.{StorageAccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider" }
                ,{ "spark.hadoop.fs.azure.account.oauth2.msi.tenant", $"{TenantId}" }
                ,{ $"spark.hadoop.fs.azure.account.auth.type.{StorageAccount}.dfs.core.windows.net", "OAuth" } };
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
    public class DatabricksCreateJobHelper
    {
        private readonly HttpClient _httpClient;
        private readonly string _databricksWorkspaceUrl;
        private readonly string _databricksToken;

        public DatabricksCreateJobHelper(string databricksToken, string databricksWorkspaceUrl)
        {
            _httpClient = new HttpClient();
            _databricksToken = databricksToken;
            _databricksWorkspaceUrl = databricksWorkspaceUrl;
        }

        public async Task<string?> CreateJobAsync(string clusterId, string jarPath, string mainClassName)
        {
            Console.WriteLine("Creating Databricks job...");
            var jobConfig = new
            {
                name = "SparkJarJob",
                tasks = new[]
                {
            new
            {
                task_key = "Task",
                description = "A Spark JAR task running on an existing cluster",
                existing_cluster_id = clusterId,
                spark_jar_task = new
                {
                    main_class_name = mainClassName
                },
                libraries = new[]
                {
                    new
                    {
                        jar = jarPath
                    }
                }
            }
        }
            };

            var requestUri = $"{_databricksWorkspaceUrl}/api/2.1/jobs/create";
            var jsonContent = System.Text.Json.JsonSerializer.Serialize(jobConfig);
            var httpContent = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _databricksToken);

            try
            {
                var response = await _httpClient.PostAsync(requestUri, httpContent);

                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("Job created successfully!");
                    var responseBody = await response.Content.ReadAsStringAsync();
                    var responseJson = System.Text.Json.JsonDocument.Parse(responseBody);
                    var jobId = responseJson.RootElement.GetProperty("job_id");

                    Console.WriteLine($"Job ID: {jobId.ToString()}");
                    return jobId.ToString();
                }
                else
                {
                    Console.WriteLine("Failed to create job.");
                    Console.WriteLine($"Status Code: {response.StatusCode}");
                    Console.WriteLine($"Response: {await response.Content.ReadAsStringAsync()}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while creating Databricks job: {ex.Message}");
                throw;
            }
        }
    }

    public class DatabricksRunJobHelper
    {
        private readonly HttpClient _httpClient;
        private readonly string _databricksWorkspaceUrl;
        private readonly string _databricksToken;

        public DatabricksRunJobHelper(string databricksToken, string databricksWorkspaceUrl)
        {
            _httpClient = new HttpClient();
            _databricksToken = databricksToken;
            _databricksWorkspaceUrl = databricksWorkspaceUrl;
        }

        public async Task<string?> RunJobAsync(string jobId, Dictionary<string, string>? Jobparameters = null)
        {
            Console.WriteLine("Running Databricks job...");

            var runConfig = new
            {
                job_id = jobId,
                job_parameters = Jobparameters
            };

            var requestUri = $"{_databricksWorkspaceUrl}/api/2.1/jobs/run-now";

            var jsonContent = System.Text.Json.JsonSerializer.Serialize(runConfig);
            var httpContent = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _databricksToken);

            try
            {
                var response = await _httpClient.PostAsync(requestUri, httpContent);

                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("Job run started successfully!");

                    Console.WriteLine("Job created successfully!");
                    var responseBody = await response.Content.ReadAsStringAsync();
                    var responseJson = System.Text.Json.JsonDocument.Parse(responseBody);
                    var runId = responseJson.RootElement.GetProperty("run_id");
                    return runId.ToString();
                }
                else
                {
                    Console.WriteLine("Failed to start job run.");
                    Console.WriteLine($"Status Code: {response.StatusCode}");
                    Console.WriteLine($"Response: {await response.Content.ReadAsStringAsync()}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while running Databricks job: {ex.Message}");
                throw;
            }
        }
    }

    public class DatabricksJobTaskHelper
        {
            private readonly HttpClient _httpClient;
            private readonly string _databricksWorkspaceUrl;
            private readonly string _databricksToken;

            public DatabricksJobTaskHelper(string databricksToken, string databricksWorkspaceUrl)
            {
                _httpClient = new HttpClient();
                _databricksToken = databricksToken;
                _databricksWorkspaceUrl = databricksWorkspaceUrl;
            }

            public async Task<object?> GetJobTaskOutputAsync(string jobRunId)
            {
            Console.WriteLine($"Retrieving task for Job Run ID: {jobRunId}...");
            if (!long.TryParse(jobRunId, out long jobRunIdLong))
            {
                throw new FormatException("Invalid Job Run ID format.");
            }
            var requestUri = $"{_databricksWorkspaceUrl}/api/2.1/jobs/runs/get?run_id={jobRunIdLong}";

            _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _databricksToken);

            try
            {
                var response = await _httpClient.GetAsync(requestUri);

                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Failed to retrieve task. Status Code: {response.StatusCode}");
                    Console.WriteLine($"Response: {await response.Content.ReadAsStringAsync()}");
                    return null;
                }

                var responseBody = await response.Content.ReadAsStringAsync();
                var runDetails = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(responseBody);
                //Console.WriteLine($"runDetails: {runDetails}"); //Entire Job Status and Details
                if (runDetails.TryGetProperty("tasks", out JsonElement tasks) && tasks.GetArrayLength() > 0)
                {
                    // Get the first task in the array
                    var firstTask = tasks[0];
                    if (firstTask.TryGetProperty("run_id", out JsonElement runIdElement))
                    {
                        return await GetTaskRunOutputAsync(runIdElement.GetInt64().ToString());
                    }  
                }
                else
                {
                    Console.WriteLine("Task run_id not found in the first task.");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while retrieving Task Id: {ex.Message}");
                throw;
            }
            return null;
        }

            private async Task<object?> GetTaskRunOutputAsync(string taskRunId)
            {
                Console.WriteLine($"Retrieving output for Task Run ID: {taskRunId}...");
            if (!long.TryParse(taskRunId, out long taskRunIdLong))
            {
                throw new FormatException("Invalid Job Run ID format.");
            }

            var requestUri = $"{_databricksWorkspaceUrl}/api/2.1/jobs/runs/get-output?run_id={taskRunIdLong}";

                try
                {
                    var response = await _httpClient.GetAsync(requestUri);

                    if (!response.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Failed to retrieve task output. Status Code: {response.StatusCode}");
                        Console.WriteLine($"Response: {await response.Content.ReadAsStringAsync()}");
                        return null;
                    }

                var responseBody = await response.Content.ReadAsStringAsync();
                var taskRunOutput = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(responseBody);

                var logs = taskRunOutput.TryGetProperty("logs", out var logsElement) ? logsElement.GetString() : null;
                var metadata = taskRunOutput.TryGetProperty("metadata", out var metadataElement) ? metadataElement : default;
                var resultState = metadata.TryGetProperty("state", out var stateElement) &&
              stateElement.TryGetProperty("result_state", out var resultStateElement)
              ? resultStateElement.GetString()
              : null;
                var lifecycleState = metadata.TryGetProperty("state", out var lifecycleElement) &&
                                     lifecycleElement.TryGetProperty("life_cycle_state", out var lifecycleElementState)
                                     ? lifecycleElementState.GetString()
                                     : null;


                Console.WriteLine($"Result State: {resultState}");
                    Console.WriteLine($"Lifecycle State: {lifecycleState}");

                    if (!string.IsNullOrEmpty(logs))
                    {
                        Console.WriteLine("Task Logs:");
                        Console.WriteLine(logs);

                        try
                        {
                            var schemaMatch = Regex.Match(logs, @"SourceAnalyzerDetectedSchema\[\[(.*?)\]\]", RegexOptions.Singleline);
                            if (schemaMatch.Success)
                            {
                                string schemaStr = $"[{schemaMatch.Groups[1].Value}]";
                            var schema = System.Text.Json.JsonSerializer.Deserialize<object>(schemaStr);
                            Console.WriteLine($"Extracted Schema: {System.Text.Json.JsonSerializer.Serialize(schema, new System.Text.Json.JsonSerializerOptions { WriteIndented = true })}");
                            return schema;
                            }
                            else
                            {
                                Console.WriteLine("No SourceAnalyzerDetectedSchema found in logs.");
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Error processing logs: {e.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("No logs available.");
                    }

                    return taskRunOutput;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error while retrieving task run output: {ex.Message}");
                    throw;
                }
            }
        }


}
