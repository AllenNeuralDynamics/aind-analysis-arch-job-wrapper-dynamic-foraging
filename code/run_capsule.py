""" top level run script """

import json
import glob
import logging
import importlib
import traceback
import os
import socket

# Define the Seattle timezone
machine_name = socket.gethostname()

from pymongo.errors import ServerSelectionTimeoutError

import multiprocessing as mp

from utils.capture_logs import capture_logs
from utils.docDB_io import (
    update_job_manager, 
    insert_result_to_docDB_ssh,
    update_CO_machine_status,
    retry_on_ssh_timeout,
    DocumentDbSSHClient,
    credentials,
)
from utils.aws_io import (
    save_fig,
    save_pkl,
    save_json,
    S3_RESULTS_ROOT,
    LOCAL_RESULTS_ROOT,
)

from utils.nwb_io import download_all_nwb_files_from_s3

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/run.log'),
                              logging.StreamHandler()])
logger = logging.getLogger()  # Use root logger to capture all logs (including logs from imported modules)

ANALYSIS_MAPPER = {
    # Mapping of analysis name to package name under analysis_wrappers
    "MLE fitting": "mle_fitting",
}


def upload_results(job_hash, results, analysis_name):
    """
    Upload results to S3

    Parameters
    ----------
    job_hash : _type_
        _description_
    results : dict
        Dictionary containing the result of the analysis.
        "status": str, "success" or others
        "upload_figs_s3": dict, figures to upload to s3, {"file_name": fig object}
        "upload_pkls_s3": dict, pkl files to upload to s3, {"pkl_name": pkl object}
        "upload_record_docDB": dict, bson-compatible record to upload to docDB
    """
    if "skipped" in results["status"]:
        return {
            "docDB_id": None,
            "docDB_upload_status": None,
            "collection_name": package_name,
            "s3_location": None,
        }

    # Upload figures to s3 (and a local copy)
    for fig_name, fig in results.get("upload_figs_s3", {}).items():
        save_fig(job_hash, fig_name, fig, if_save_local=True)

    # Upload pkl files to s3 (and a local copy)
    for pkl_name, pkl in results.get("upload_pkls_s3", {}).items():
        save_pkl(job_hash, pkl_name, pkl, if_save_local=True)

    upload_status = {"s3_location": "to_be_filled"} # f"s3://{S3_RESULTS_ROOT}/{job_hash}"}

    # Save docDB record local
    upload_record_docDB = results.get("upload_record_docDB", {})
    save_json(
        job_hash=job_hash,
        filename=f"docDB_{analysis_name}.json",
        dict=upload_record_docDB,
        if_save_local=True,
    )
    msg = f"Save results done! {'-' * 20}"
    logger.info(msg)
    print(msg, flush=True)

    # Upload record to docDB
    # try:
    #     upload_status_docDB = insert_result_to_docDB_ssh(
    #             result_dict=upload_record_docDB, 
    #             collection_name="mle_fitting",
    #             doc_db_client=doc_db_client,
    #     )  # Note that this will add _id automatically to upload_record_docDB
    #     msg = f"Insert docDB done! {'-' * 20}"
    #     logger.info(msg)
    #     print(msg, flush=True)
        
    # except Exception as e:
    #     upload_status.update(
    #         {
    #             "docDB_upload_status": "failed; too many SSH Errors",
    #             "docDB_id": None,
    #             "collection_name": None,
    #         }
    #     )
    #     return upload_status
        
    # upload_status.update(upload_status_docDB)  
    return upload_status

def _run_one_job(job_file, parallel_inside_job):
    with open(job_file) as f:
        job_dict = json.load(f)

    job_hash = job_dict["job_hash"]

    # Get analysis function
    package_name = ANALYSIS_MAPPER[job_dict["analysis_spec"]["analysis_name"]]
    analysis_fun = importlib.import_module(f"analysis_wrappers.{package_name}").wrapper_main

    try:
        # -- Trigger analysis --
        logger.info("")
        msg = f"Running {job_dict['analysis_spec']['analysis_name']} for {job_dict['nwb_name']}"
        logger.info(msg)
        print(msg, flush=True)
        logger.info(f"Job hash: {job_hash}")

        # Update status to "running" in job manager DB
        # update_job_manager(job_hash=job_hash, update_dict={"status": "running"})

        analysis_results = capture_logs(logger)(analysis_fun)(job_dict, parallel_inside_job)
        results, log = analysis_results["result"], analysis_results["logs"]
        logger.info(
            f"Job {job_hash} completed with status: {results['status']}"
        )
        print(f"Job {job_hash} completed with status: {results['status']}", flush=True)  # Print to console of CO pipeline run

        # -- Upload results --
        upload_response = capture_logs(logger)(upload_results)(job_hash, results, package_name)

        upload_status, upload_log = upload_response["result"], upload_response["logs"]
        log += upload_log  # Also add log during upload

        # -- Update job manager DB with log and status --
        # update_job_manager(
        #     job_hash,
        #     update_dict={
        #         "status": results["status"],
        #         "docDB_upload_status": upload_status["docDB_upload_status"],
        #         "docDB_id": upload_status["docDB_id"],
        #         "collection_name": upload_status["collection_name"],
        #         "s3_location": upload_status["s3_location"],
        #         "log": log,
        #     },
        #     doc_db_client=doc_db_client,
        # )
        save_json(
            job_hash=job_hash,
            filename="docDB_job_manager.json",
            dict={
                "status": results["status"],
                "docDB_id": "to_be_filled",
                "collection_name": package_name,
                "s3_location": upload_status.get("s3_location", None),
                "log": log,
            },
            if_save_local=True,
        )

    except Exception as e:  # Unhandled exception
        logger.error(f"Job {job_hash} failed with unhandled exception: {e}")
        logger.error(traceback.format_exc())  # Logs the full traceback
        print(traceback.format_exc(), flush=True)  # For CO console

        # try:
        #     update_job_manager(
        #         job_hash,
        #         update_dict={
        #             "status": "failed due to unhandled exception (see log)",
        #             "docDB_id": None,
        #             "collection_name": None,
        #             "log": log,
        #         },
        #         doc_db_client=doc_db_client,
        #     )
        # except:
        #     logger.error("'Failed' message failed to upload...")
        save_json(
            job_hash=job_hash,
            filename="docDB_job_manager.json",
            dict={
                "status": "failed due to unhandled exception (see log)",
                "docDB_id": None,
                "collection_name": package_name,
                "log": log,
            },
            if_save_local=True,
        )

# @retry_on_ssh_timeout()
# def batch_run_with_single_ssh_and_retry(batch_i, job_files_this_batch):
#     with DocumentDbSSHClient(credentials=credentials) as client:  # This is only ssh connection to retry
#         # Send a message to the CO_machine_log collection
#         update_CO_machine_status(
#             machine_name, batch_i,
#             status_type="batch",
#             status="start",
#             doc_db_client=client
#         )

#         for j, job_file in enumerate(job_files_this_batch):
#             job_hash = os.path.basename(job_file).replace(".json", "")

#             update_CO_machine_status(
#                 machine_name,
#                 batch_i,
#                 job_hash_name=f"{j+1}/{len(job_files_this_batch)}_{job_hash}",
#                 status_type="job",
#                 status="running",
#                 doc_db_client=client,
#             )

#             _run_one_job(job_file, parallel_inside_job=True, doc_db_client=client)

#             # Update progress
#             update_CO_machine_status(
#                 machine_name,
#                 batch_i,
#                 job_hash_name=f"{j+1}/{len(job_files_this_batch)}_{job_hash}",
#                 status_type="job",
#                 status="success",
#                 doc_db_client=client,
#             )

def run(parallel_on_jobs=False, debug_mode=True, docDB_ssh_batch_size=50):
    """
    Parameters
    -----
    parallel_on_jobs, boolean, Optional (by default, True)
        if true, will call multiprocessing on the level of job
        else, process each job sequentially, but go parallel inside each job (e.g., DE workers)
    """
    # Discover all job json in /root/capsule/data
    job_files = glob.glob(f"{SCRIPT_DIR}/../data/jobs/**/*.json", recursive=True)

    if debug_mode:
        job_files = job_files[:1]
        
    logger.info(f"The machine name is: {machine_name}")

    # Download all needed nwb files from s3
    download_all_nwb_files_from_s3(job_files)

    # For each job json, run the corresponding job using multiprocessing
    if parallel_on_jobs:
        cpu_count = int(os.getenv("CO_CPUS"))
        pool = mp.Pool(cpu_count)
        logger.info(f"\n\nRunning {len(job_files)} jobs, parallel on jobs with {cpu_count} workers...")
        results = [pool.apply_async(_run_one_job, args=(job_file, False)) for job_file in job_files]
        _ = [r.get() for r in results]
        pool.close()
        pool.join()
    else:
        logger.info(f"\n\nRunning {len(job_files)} jobs, serial on jobs...")
        [_run_one_job(job_file, parallel_inside_job=True) for job_file in job_files]

        # # Open ssh channel once for docDB_ssh_batch_size jobs to reduce ssh overhead
        # for i in range(0, len(job_files), docDB_ssh_batch_size):
        #     msg = f"---- Start batch {i+1} ----"
        #     logger.info(msg)
        #     print(msg, flush=True)
            
        #     job_files_this_batch = job_files[i:i+docDB_ssh_batch_size]
            
        #     # Batch run with single ssh connection with retry
        #     batch_run_with_single_ssh_and_retry(i, job_files_this_batch)


    logger.info(f"All done!")

if __name__ == "__main__": 

    import argparse

    # create a parser object
    parser = argparse.ArgumentParser()
    
    # add the corresponding parameters
    parser.add_argument('--parallel_on_jobs', dest='parallel_on_jobs')
    parser.add_argument('--debug_mode', dest='debug_mode')
    
    # return the data in the object and save in args
    args = parser.parse_args()

    # retrive the arguments
    parallel_on_jobs = bool(int(args.parallel_on_jobs or "0"))  # Default 0
    debug_mode = bool(int(args.debug_mode or "1"))  # Default 1

    run(parallel_on_jobs=parallel_on_jobs, debug_mode=debug_mode)
