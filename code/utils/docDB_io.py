import logging
import time
from functools import wraps
from pymongo.errors import ServerSelectionTimeoutError
from sshtunnel import HandlerSSHTunnelForwarderError

SSH_ERRORS = [ServerSelectionTimeoutError, HandlerSSHTunnelForwarderError]

import traceback
from datetime import datetime

from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"

logger = logging.getLogger(__name__)

MAX_SSH_RETRIES = 50
TIMEOUT = 5 # Time in seconds between retries

def retry_on_ssh_timeout(max_retries=MAX_SSH_RETRIES, timeout=TIMEOUT):
    """
    Decorator to retry a function upon ServerSelectionTimeoutError.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                # Suppose all other exceptions have been captured inside func!
                except SSH_ERRORS as e:  
                    retries += 1
                    msg = f"{str(e)}\n{traceback.format_exc()}\nRetry {retries}/{max_retries}..."
                    logger.warning(msg)
                    print(msg, flush=True)
                    if retries >= max_retries:
                        logger.error("Max retries reached. Raising exception.")
                        raise
                    time.sleep(timeout)
        return wrapper
    return decorator

def insert_result_to_docDB_ssh(result_dict, collection_name, doc_db_client) -> dict:
    """_summary_

    Parameters
    ----------
    result_dict : dict
        bson-compatible result dictionary to be inserted
    collection_name : _type_
        name of the collection to insert into (such as "mle_fitting")

    Returns
    -------
    dict
        docDB upload status
    """
    doc_db_client.collection_name = collection_name
    db = doc_db_client.collection

    # Check if job hash already exists, if yes, log warning, but still insert
    if db.find_one({"job_hash": result_dict["job_hash"]}):
        logger.warning(f"Job hash {result_dict['job_hash']} already exists in {collection_name} in docDB")
    # Insert (this will add _id automatically to result_dict)
    response = db.insert_one(result_dict)
    result_dict["_id"] = str(result_dict["_id"])

    if response.acknowledged is False:
        logger.error(f"Failed to insert {result_dict['job_hash']} to {collection_name} in docDB")
        return {"docDB_upload_status": "docDB insertion not acknowledged", "docDB_id": None, "collection_name": None}
    else:
        logger.info(f"Inserted {response.inserted_id} to {collection_name} in docDB")
        return {"docDB_upload_status": "success", "docDB_id": response.inserted_id, "collection_name": collection_name}


def update_CO_machine_status(
    machine_name, batch_i, doc_db_client, job_hash_name="", status_type="batch", status="",
):
    """Send status to collection CO_machine_status for monitor CO pipeline"""
    doc_db_client.collection_name = "CO_machine_status"
    db = doc_db_client.collection

    if status_type == "batch":
        db.update_one(
            {"machine": machine_name},
            {
                "$set": {
                    f"batch_{batch_i}.{status}": datetime.now(),
                }
            },
            upsert=True,
        )
    elif status_type == "job":
        db.update_one(
            {"machine": machine_name},
            {
                "$set": {
                    f"batch_{batch_i}.{job_hash_name}": status,
                }
            },
            upsert=True,
        )


def update_job_manager(job_hash, update_dict, doc_db_client):
    """_summary_

    Parameters
    ----------
    job_hash : _type_
        _description_
    status : _type_
        _description_
    log : _type_
        _description_
    """
    doc_db_client.collection_name = "job_manager"
    db = doc_db_client.collection
    
    # Check if job hash already exists, if yes, log warning, but still insert
    if not db.find_one({"job_hash": job_hash}):
        logger.warning(f"Job hash {job_hash} does not exist in job_manager in docDB! Skipping update.")
        return
    
    # Update job status and log
    response = db.update_one(
        {"job_hash": job_hash},
        {"$set": update_dict},
    )
