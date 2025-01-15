import requests
from azure.identity import DefaultAzureCredential
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.resource import ResourceManagementClient

# Constants
SUBSCRIPTION_ID = "<>"
RESOURCE_GROUP = "adqueryvnettestrg"
LOCATION = "uksouth"
WORKSPACE_NAME = "adbworkspacedev01"
DATABRICKS_WORKSPACE_URL = "https://adb-<>.<>.azuredatabricks.net"
JAR_STORAGE_ACCOUNT = "adlsstoragedev01"
TENANT_ID = "<>"

# Azure Clients
credential = DefaultAzureCredential()
resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)
databricks_client = AzureDatabricksManagementClient(credential, SUBSCRIPTION_ID)

# Obtain an Azure AD Token for Databricks
def get_databricks_aad_token():
    aad_token = credential.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d").token
    return aad_token


def get_databricks_managed_identity_client_id():
    """Retrieve the Managed Identity Client ID for the Databricks Workspace."""
    print("Retrieving Databricks Managed Identity Client ID...")
    
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
        return managed_identity.properties["clientId"]
    except Exception as e:
        raise ValueError("Failed to retrieve the Managed Identity Client ID.")

MSI_CLIENT_ID = get_databricks_managed_identity_client_id()

# Create Cluster
def create_cluster(databricks_token):
    print("Creating Databricks cluster...")
    cluster_config = {
        "cluster_name": "StandardCluster",
        "spark_version": "16.1.x-scala2.12",  # Replace with desired Databricks Runtime version
        "node_type_id": "Standard_D4ds_v5",
        "num_workers": 2,
        "autotermination_minutes": 30,  # Auto-terminate after 30 minutes of inactivity
        "spark_conf": {
        "spark.hadoop.fs.azure.account.oauth2.client.id": MSI_CLIENT_ID,
        f"spark.hadoop.fs.azure.account.oauth.provider.type.{JAR_STORAGE_ACCOUNT}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
        "spark.hadoop.fs.azure.account.oauth2.msi.tenant": TENANT_ID,
        f"spark.hadoop.fs.azure.account.auth.type.{JAR_STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth"
    }
    }
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(f"{DATABRICKS_WORKSPACE_URL}/api/2.0/clusters/create", headers=headers, json=cluster_config)
    if response.status_code == 200:
        print("Cluster created successfully!")
        cluster_id = response.json().get("cluster_id")
        print(f"Cluster ID: {cluster_id}")
        return cluster_id
    else:
        print("Failed to create cluster.")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return

#createJob
def create_job(databricks_token, cluster_id):
    """
    Create a Databricks job mapped to an existing cluster.
    """
    print("Creating Databricks job...")
    
    # Job Configuration
    job_config = {
        "name": "SparkJarJob",
        "tasks": [
            {
                "task_key": "Task",
                "description": "A Spark JAR task running on an existing cluster",
                "existing_cluster_id": cluster_id,  # Use existing cluster
                "spark_jar_task": {
                    "main_class_name": "org.proj.deltamain"  # Main class to execute
                },
                "libraries": [
                    {
                        "jar": "abfss://jarcontainer@adlsstoragedev01.dfs.core.windows.net/jardir/default_artifact.jar"
                    }
                ]
            }
        ]
    }
    # API Headers
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    # API Call to Create Job
    response = requests.post(f"{DATABRICKS_WORKSPACE_URL}/api/2.1/jobs/create", headers=headers, json=job_config)
    # Handle Response
    if response.status_code == 200:
        print("Job created successfully!")
        job_id = response.json().get("job_id")
        print(f"Job ID: {job_id}")
        return job_id
    else:
        print("Failed to create job.")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return None

# Run Job
def run_job(databricks_token, job_id):
    print("Running Databricks job...")
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    run_config = {
        "job_id": job_id
    }
    response = requests.post(f"{DATABRICKS_WORKSPACE_URL}/api/2.1/jobs/run-now", headers=headers, json=run_config)
    if response.status_code == 200:
        print("Job run started successfully!")
        run_id = response.json().get("run_id")
        print(f"Run ID: {run_id}")
        return run_id
    else:
        print("Failed to start job run.")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return None
    

# Main Execution
try:
    # Step 1: Get Databricks AAD Token
    databricks_token = get_databricks_aad_token()
    # Step 2: Use the token to create a cluster
    cluster_id = create_cluster(databricks_token)
    print(f"Cluster Created With ClusterId {cluster_id}")
    if cluster_id:
        job_id = create_job(databricks_token, cluster_id)
        print(f"Job Created With JobId {job_id}")
    # Step 4: Run the Job (Commented for now)
    if job_id:
        run_id = run_job(databricks_token, job_id)
        print(f"Run Created With RunId {run_id}")

except Exception as e:
    print(f"Error: {e}")
