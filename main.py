import json
import logging
from powerbi_refresh_manager import PowerBIRefreshManager


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Main logic
try:
    # Read widget inputs
    scope = dbutils.widgets.get("scope")
    client_id_key = dbutils.widgets.get("client_id_key")
    client_secret_key = dbutils.widgets.get("client_secret_key")
    tenant_id_key = dbutils.widgets.get("tenant_id_key")
    workspace_id = dbutils.widgets.get("workspace_id")
    dataset_id = dbutils.widgets.get("dataset_id")
    poll_interval = int(dbutils.widgets.get("poll_interval"))
    refresh_objects_raw = dbutils.widgets.get("refresh_objects")

    # Resolve secrets
    client_id = dbutils.secrets.get(scope=scope, key=client_id_key)
    client_secret = dbutils.secrets.get(scope=scope, key=client_secret_key)
    tenant_id = dbutils.secrets.get(scope=scope, key=tenant_id_key)

    # Validate and parse refresh objects
    refresh_objects = json.loads(refresh_objects_raw)
    if not isinstance(refresh_objects, list):
        raise ValueError("refresh_objects must be a JSON list of objects")

    # Initialize manager and run
    manager = PowerBIRefreshManager(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        refresh_objects=refresh_objects
    )

    manager.trigger_refresh()
    status, start_time, end_time, total_time = manager.wait_for_refresh_completion(poll_interval)

    logger.info(f"Refresh Completed with Status: {status}")
    logger.info(f"Started at: {start_time}")
    logger.info(f"Ended at: {end_time}")
    logger.info(f"Total Duration: {total_time} seconds")

except Exception as e:
    logger.exception(f"Process failed: {str(e)}")
