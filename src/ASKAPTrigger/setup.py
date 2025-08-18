# functions for setting things up...

import os
import shutil
import importlib.resources
import ASKAPTrigger

import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def _check_config_file(fname):
    if os.path.exists(f"./{fname}"):
        return True
    logger.info(f"file {fname} not found in the current directory...")
    return False

def _copy_config_file(fname):
    fpath = importlib.resources.path("ASKAPTrigger", fname)
    if not _check_config_file(fname):
        logger.info(f"copying file {fname} to the current directory...")
        shutil.copyfile(os.fspath(fpath), f"./{fname}")

def setup():
    configfiles = ["askap_trigger_config.json", "trigger_mwa_config.json"]
    for configfile in configfiles:
        _copy_config_file(configfile)
