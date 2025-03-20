import logging
import os
import time
import pkg_resources
from datetime import datetime

import numpy as np
import multiprocessing as mp

from utils.nwb_io import get_history_from_nwb, get_nwb_from_local_tmp
from aind_dynamic_foraging_models.generative_model import ForagerCollection

logger = logging.getLogger(__name__)

def wrapper_main(job_dict, parallel_inside_job=False) -> dict:
    """Main entrance of this analysis
    Note that the function name should be wrapper_main()
    The job dispatcher will look for this function (by file name) to trigger the analysis.
    
    Parameters
    ----------
    job_dict : dict
        Dictionary containing job information (nwb and analysis_spec)
    parallel_inside_job : bool, optional
        Whether to run parallel computation inside the job, by default False
        If false, DE_workers will be set to 1.
        
    Returns
    -------
    dict
        Dictionary containing the result of the analysis.
        Required fields:
            "status": str, "success" or others
            "upload_figs_s3": dict, figures to upload to s3, {"file_name": fig object}
            "upload_pkls_s3": dict, pkl files to upload to s3, {"pkl_name": pkl object}
            "upload_record_docDB": dict, bson-compatible record to upload to docDB
    TODO: use pydantic to validate the input and output
    """
    start_time = time.time()

    job_hash = job_dict["job_hash"]
    nwb_name = job_dict["nwb_name"]
    analysis_args = job_dict["analysis_spec"]["analysis_args"]

    # Overwrite DE_workers
    if parallel_inside_job:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = int(os.getenv("CO_CPUS"))
    else:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = 1

    logger.info(f"MLE fitting for {nwb_name} with {analysis_args['agent_class']}")

    # -- Load data --
    session_id = job_dict["nwb_name"].replace(".nwb", "")
    nwb = get_nwb_from_local_tmp(session_id=session_id)
    (
        baiting,
        choice_history,
        reward_history,
        _,
        autowater_offered,
        random_number,
    ) = get_history_from_nwb(nwb)

    # Remove NaNs
    ignored = np.isnan(choice_history)
    choice_history = choice_history[~ignored]
    reward_history = reward_history[~ignored].to_numpy()

    # -- Skip if len(valid trials) < 50 --
    if len(choice_history) < 50:
        return {
            "status": "skipped. valid trials < 50",
            "upload_figs_s3": {},
            "upload_pkls_s3": {},
            "upload_record_docDB": {},
        }

    # -- Initialize model --
    forager = ForagerCollection().get_forager(
       agent_class_name=analysis_args["agent_class"],
       agent_kwargs=analysis_args["agent_kwargs"],
    )
    forager.fit(
       choice_history,
       reward_history,
       **analysis_args["fit_kwargs"],
    )

    # -- Saving results --
    upload_figs_s3 = {}
    upload_pkls_s3 = {}
    
    # 1. Figure
    result_dir = f"/root/capsule/results/{job_hash}"
    os.makedirs(result_dir, exist_ok=True)

    fig_fitting, _ = forager.plot_fitted_session(if_plot_latent=True)
    upload_figs_s3["fitted_session.png"] = fig_fitting
 
    # 2. Fit results object
    # Have to flatten pydantic models in forager for pickle to work
    forager.ParamModel = forager.ParamModel.model_json_schema()
    forager.ParamFitBoundModel = forager.ParamFitBoundModel.schema_json()
    forager.params = forager.params.model_dump()
    upload_pkls_s3["forager.pkl"] = forager

    # 3. Database record --
    analysis_results = forager.get_fitting_result_dict()

    analysis_libs_to_track_ver = {
        lib: pkg_resources.get_distribution(lib).version
        for lib in job_dict["analysis_spec"]["analysis_libs_to_track_ver"]
    }

    upload_record_docDB = {
        **job_dict,
        "analysis_datetime": datetime.now().isoformat(),
        "analysis_time_spent_in_sec": time.time() - start_time, 
        "analysis_libs_to_track_ver": analysis_libs_to_track_ver,
        "analysis_results": analysis_results,
    }

    return {
        "status": "success",
        "upload_figs_s3": upload_figs_s3,
        "upload_pkls_s3": upload_pkls_s3,
        "upload_record_docDB": upload_record_docDB,
    }