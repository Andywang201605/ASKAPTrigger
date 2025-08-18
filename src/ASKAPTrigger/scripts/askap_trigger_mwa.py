#!/usr/bin/python

import os

from ASKAPTrigger.ASKAPTriggerMWA import ASKAPMWATrigger

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Schedule MWA trigger observations based on ASKAP observation SBID', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--sbid", type=int, help="ASKAP schedule block ID", default=None)
    parser.add_argument("-p", "--pid", type=str, help="MWA project ID for the triggered observations", default="T001")
    parser.add_argument("--dryrun", action="store_true", help="whether run as a dry run or not", default=False)
    values = parser.parse_args()

    os.makedirs("./log", exist_ok=True)

    import logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        level=logging.INFO,
        handlers=[
            logging.FileHandler(f"./log/{values.sbid}.mwatrigger.log"),
            logging.StreamHandler()
        ],
    )
    logger = logging.getLogger(__name__)

    trigger = ASKAPMWATrigger(sbid=values.sbid, project_id=values.pid, dryrun=values.dryrun)
    trigger.run()