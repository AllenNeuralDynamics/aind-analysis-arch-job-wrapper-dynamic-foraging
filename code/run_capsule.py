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

from utils.save_results import (
    save_fig,
    save_pkl,
    save_json,
)

from utils.nwb_io import download_all_nwb_files_from_s3

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[# logging.FileHandler(f'{SCRIPT_DIR}/../results/run.log'),  # Remove logging file to avoid pipeline conflict
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
            "collection_name": None,
            "s3_location": None,
        }

    # Upload figures to s3 (and a local copy)
    for fig_name, fig in results.get("upload_figs_s3", {}).items():
        save_fig(job_hash, fig_name, fig, if_save_local=True)

    # Upload pkl files to s3 (and a local copy)
    for pkl_name, pkl in results.get("upload_pkls_s3", {}).items():
        save_pkl(job_hash, pkl_name, pkl, if_save_local=True)

    upload_status = {"s3_location": "to_be_filled"}

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

        analysis_results = analysis_fun(job_dict, parallel_inside_job)
        results = analysis_results["result"]
        logger.info(
            f"Job {job_hash} completed with status: {results['status']}"
        )
        print(f"Job {job_hash} completed with status: {results['status']}", flush=True)  # Print to console of CO pipeline run

        # -- Upload results --
        upload_response = upload_results(job_hash, results, package_name)
        upload_status = upload_response["result"]

        save_json(
            job_hash=job_hash,
            filename="docDB_job_manager.json",
            dict={
                "status": results["status"],
                "docDB_id": "to_be_filled",
                "collection_name": package_name,
                "s3_location": "to_be_filled",
            },
            if_save_local=True,
        )

    except Exception as e:  # Unhandled exception
        logger.error(f"Job {job_hash} failed with unhandled exception: {e}")
        logger.error(traceback.format_exc())  # Logs the full traceback
        print(traceback.format_exc(), flush=True)  # For CO console

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
