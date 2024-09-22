import s3fs
import pickle
import json
import logging
import os

S3_RESULTS_ROOT = "aind-scratch-data/aind-dynamic-foraging-analysis"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LOCAL_RESULTS_ROOT = f"{SCRIPT_DIR}/../../results"

fs = s3fs.S3FileSystem(anon=False)

logger = logging.getLogger(__name__)

def save_fig(job_hash, filename, fig, if_save_local=True):
    #with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
    #    fig.savefig(f)
    #    logger.info(f"Uploaded {filename} to S3")
    
    if if_save_local:
        os.makedirs(f"{LOCAL_RESULTS_ROOT}/{job_hash}", exist_ok=True)
        fig.savefig(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}")
        logger.info(f"Saved {filename} locally")

def save_pkl(job_hash, filename, obj, if_save_local=True):
    #with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
    #    pickle.dump(obj, f)
    #    logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        os.makedirs(f"{LOCAL_RESULTS_ROOT}/{job_hash}", exist_ok=True)
        with open(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
            pickle.dump(obj, f)
            logger.info(f"Saved {filename} locally")
        
    """
    # -- Reload from pickle to recover forager.pkl --
    with fs.open(f"{s3_results_root}/{job_hash}/forager.pkl", "rb") as f:
        forager_reloaded = pickle.load(f)
        
    # Recover pydantic models if needed
    forager_tmp = forager_reloaded.__class__(**forager_reloaded.agent_kwargs)
    forager_reloaded.ParamModel = forager_tmp.ParamModel
    forager_reloaded.ParamFitBoundModel = forager_tmp.ParamFitBoundModel
    forager.params = forager_reloaded.ParamModel(**forager.params)
    
    # Test
    forager.plot_fitted_session(if_plot_latent=True)
    """

def save_json(job_hash, filename, dict, if_save_local=True):
    #with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "w") as f:
    #    json.dump(dict, f, indent=4)
    #    logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        os.makedirs(f"{LOCAL_RESULTS_ROOT}/{job_hash}", exist_ok=True)
        with open(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}", "w") as f:
            json.dump(dict, f, indent=4)
            logger.info(f"Saved {filename} locally")