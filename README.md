Complete/Selective Refresh Power BI Model from Azure Databricks

🔍 Introduction
An important aspect of migration to Databricks is ensuring effective data refresh for external BI tools like Power BI or Tableau. This document outlines the method to perform a complete or selective refresh of Power BI Import Mode Models using Power BI REST APIs from Azure Databricks.
One can opt for refreshing the entire model or refresh selective tables in the model through Enhanced Refresh of Power BI with Rest API eliminating timeout issues.

🧰 Pre-requisites
Azure Admin and Power BI Admin privileges.
Azure Service Principal (SPN) with necessary API permissions.
Power BI Tenant configured to allow service principals.





🔐 Step 1: Service Principal Setup:

Let’s set up the SPN and its necessary permissions needed to trigger a refresh in PowerBI Workspace.

Create a SPN In Azure App Registrations.







Go to API permissions of the App created above and add the below permissions 
		
DatasetReadWrite.All




Then create a secret and a key. We need to use this secret and its key in our code to trigger the refresh.





🏢 Step 2: Power BI Tenant Configuration:

Open Power BI Tenant and add the SPN as Admin in the Manage Access Tab.



Once Done, go to the Power BI Admin Portal and Enable “Allow Service Principals to use Power BI APIs” option. You can enable it for an entire organization or specific security groups based on your security needs.







🧾 Step 3: Code for Refresh Operations:


Pre-requisites: 

Client Secret = Secret Key of the SPN Created Above
Client ID = App ID Of the SPN Created above
Tenant ID = Azure Tenant Name 

Reference of how to get above details are mentioned below : 



For maintaining standard security practices, store the client_secret, client_id and the tenant_id in a databricks scope and use the scope keys in the refresh code.
 
Workspace Id : Group Id of the Power BI Workspace.
Dataset ID : Dataset ID of the dataset in Power BI Workspace.


We can get the workspace id and dataset id details from Power BI Model:






Below Code does the following:

Gets the authentication token from azure
Triggers the Refresh API (Selective/Complete)
Fetches the Refresh Completion Start Time, End Time, Total Time Elapsed

import requests
import json
import msal
import time
from datetime import datetime


session = requests.Session()




# === Utility to acquire token ===
def get_access_token(client_id, client_secret, tenant_id):
   scope = ['https://analysis.windows.net/powerbi/api/.default']
   app = msal.ConfidentialClientApplication(
       client_id=client_id,
       client_credential=client_secret,
       authority=f'https://login.microsoftonline.com/{tenant_id}'
   )
   result = app.acquire_token_for_client(scopes=scope)
   if 'access_token' not in result:
       raise Exception("Failed to acquire access token")
   return result['access_token']




# === Method 1: Trigger Refresh ===
def trigger_refresh(workspace_id, dataset_id, client_id, client_secret, tenant_id):
   access_token = get_access_token(client_id, client_secret, tenant_id)


   refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
   headers = {
       'Authorization': f'Bearer {access_token}',
       'Content-Type': 'application/json'
   }
   payload = {
       "type": "Full",
       "commitMode": "transactional",
       "maxParallelism": 2,
       "retryCount": 2,
       "timeout": "02:00:00",
       "objects": [
           {"table": "DimCustomer", "partition": "DimCustomer"},
           {"table": "DimDate"}
       ]
   }


   response = requests.post(refresh_url, headers=headers, data=json.dumps(payload))
   print(f"Trigger Refresh Response: {response.status_code} - {response.text}")


   if response.status_code != 202:
       raise Exception(f"Failed to trigger refresh: {response.status_code} - {response.text}")


   return True  # Just acknowledge it triggered




# === Method 2: Check Refresh Status Until Done ===
def wait_for_refresh_completion(workspace_id, dataset_id, client_id, client_secret, tenant_id, poll_interval=10):
   access_token = get_access_token(client_id, client_secret, tenant_id)


   status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
   headers = {
       'Authorization': f'Bearer {access_token}',
       'Content-Type': 'application/json'
   }


   start_time = datetime.now()
   print(f"Started polling at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")


   while True:
       response = requests.get(status_url, headers=headers)
       if response.status_code != 200:
           raise Exception(f"Failed to fetch refresh status: {response.status_code} - {response.text}")


       refresh_data = response.json()
       latest_refresh = refresh_data.get('value', [])[0]


       status = latest_refresh.get('status', 'Unknown')
       print(f"Current Refresh Status: {status} at {datetime.now().strftime('%H:%M:%S')}")


       if status.lower() not in ["unknown", "inprogress"]:
           # If status is not 'InProgress' or 'Unknown', we're done
           end_time = datetime.now()
           total_time = (end_time - start_time).total_seconds()
           print(f"Finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
           print(f"Total Refresh Time: {total_time:.2f} seconds")
           print(f"Final Status: {status}")
           return status, start_time, end_time, total_time


       # Sleep and check again
       time.sleep(poll_interval)


# === Example Usage ===


# == Credentials ==
#client_secret = dbutils.secrets.get(scope="",key="")
client_id = ""
client_secret = ""
tenant_id = ""
workspace_id = ""
dataset_id = ""


# == Step 1: Trigger the refresh ==
trigger_refresh(workspace_id, dataset_id, client_id, client_secret, tenant_id)


# == Step 2: Keep polling until it's complete ==
status, start_time, end_time, total_time = wait_for_refresh_completion(
   workspace_id, dataset_id, client_id, client_secret, tenant_id, poll_interval=10
)


print(f"\nRefresh Completed with Status: {status}")
print(f"Started at: {start_time}")
print(f"Ended at: {end_time}")
print(f"Total Duration: {total_time} seconds")





Once triggered we can verify the refresh operation in Power BI:





To perform a selective refresh, you can use the provided code with a payload that specifies the tables (or partitions) you want to refresh. This allows for more granular control, enabling refresh operations at the table or partition level instead of refreshing the entire model.
If you wish to refresh the entire dataset, simply omit the payload from the POST request—this will trigger a full model refresh by default


##response = requests.post(api_url, headers=headers)
