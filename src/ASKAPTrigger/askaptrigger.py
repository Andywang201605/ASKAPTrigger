#!/usr/bin/python

# classes for triggers from askap side
# this would be the main function to be running all the time

import Ice
import IceStorm
from askap.iceutils import get_service_object
from aces.askapdata.schedblock import SchedulingBlock

import askap.interfaces as iceint
from askap.interfaces.schedblock import ObsState

from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy import units

import re
import os
import sys
import json
import subprocess

import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

class SBStateSubscriber(object):
    def __init__(self, monitor_impl=None):
        self.topic_name = "sbstatechange"
        self.manager = None
        if monitor_impl is None: 
            self.monitor_impl = iceint.schedblock.ISBStateMonitor
        else:
            self.monitor_impl = monitor_impl
        self.ice = self._setup_communicator()
        self._setup_subscriber()

    def _setup_subscriber(self):
        self.manager = get_service_object(
            self.ice,
            'IceStorm/TopicManager@IceStorm.TopicManager',
            IceStorm.TopicManagerPrx
        )
        try:
            self.topic = self.manager.retrieve(self.topic_name)
        except IceStorm.NoSuchTopic:
            try:
                self.topic = self.manager.create(self.topic_name)
            except IceStorm.TopicExists:
                self.topic = self.manager.retrieve(self.topic_name)
        # defined in config.icegrid
        self.adapter = self.ice.createObjectAdapterWithEndpoints(
            "SBStateSubAdapter", "tcp")

        self.subscriber = self.adapter.addWithUUID(self.monitor_impl).ice_oneway()
        qos = {}
        try:
            self.topic.subscribeAndGetPublisher(qos, self.subscriber)
        except IceStorm.AlreadySubscribed:
            raise
        self.adapter.activate()


    @staticmethod
    def _setup_communicator():
        if "ICE_CONFIG" in os.environ:
            return Ice.initialize(sys.argv)
        host = 'localhost'
        port = 4062
        init = Ice.InitializationData()
        init.properties = Ice.createProperties()
        loc = "IceGrid/Locator:tcp -h " + host + " -p " + str(port)
        init.properties.setProperty('Ice.Default.Locator', loc)
        init.properties.setProperty('Ice.IPv6', '0')
        return Ice.initialize(init)
    
class ASKAPSchedBlock:
    
    def __init__(self, sbid):
        self.sbid = sbid
        self._refresh_schedblock()

    # get source field direction
    def _get_field_direction(self, src="src1"):
        ### first try common.target.src?.field_direction in obsparams
        if f"common.target.{src}.field_direction" in self.obsparams:
            field_direction_str = self.obsparams[f"common.target.{src}.field_direction"]
        ### then try schedblock.src16.field_direction in obsvar
        elif f"schedblock.{src}.field_direction" in self.obsvar:
            field_direction_str = self.obsvar[f"schedblock.{src}.field_direction"]
        return self.__parse_field_direction(field_direction_str)
            
    def __parse_field_direction(self, field_direction_str):
        """parse field_direction_str"""
        pattern = """\[(.*),(.*),.*\]"""
        matched = re.findall(pattern, field_direction_str)
        assert len(matched) == 1, f"find none or more matched pattern in {field_direction_str}"
        ### then further parse ra and dec value
        ra_str, dec_str = matched[0]
        ra_str = ra_str.replace("'", "").replace('"', "") # replace any possible " or '
        dec_str = dec_str.replace("'", "").replace('"', "")
        
        if (":" in ra_str) and (":" in dec_str):
            field_coord = SkyCoord(ra_str, dec_str, unit=(units.hourangle, units.degree))
        else:
            field_coord = SkyCoord(ra_str, dec_str, unit=(units.degree, units.degree))
            
        return field_coord.ra.value, field_coord.dec.value
    
    def get_scan_source(self):
        """
        retrieve scan and source pair based on the schedulingblock
        """
        refant = self.antennas[0]
        scan_src_match = {}
        sources = []
        for scan in range(100): # assume maximum scan number is 99
            scanstr = f"{scan:0>3}"
            scanantkey = f"schedblock.scan{scanstr}.target.{refant}"
            if scanantkey in self.obsvar: 
                src = self._find_scan_source(scan)
                scan_src_match[scan] = src
                if src not in sources: sources.append(src)
            else: break
        self.scan_src_match = scan_src_match
        self.sources = sources
            
    def _find_scan_source(self, scan):
        # in self.obsvar under schedblock.scan000.target.ant1
        scanstr = f"{scan:0>3}"
        allsrc = [self.obsvar[f"schedblock.scan{scanstr}.target.{ant}"].strip() for ant in self.antennas]
        unisrc = list(set(allsrc))
        assert len(unisrc) == 1, "cannot handle fly's eye mode..."
        return unisrc[0]
        
    def get_sources_coord(self, ):
        """
        get source and direction pair
        """
        self.get_scan_source()
        self.source_coord = {src:self._get_field_direction(src) for src in self.sources}

    def _refresh_schedblock(self,):
        try: self.askap_schedblock = SchedulingBlock(self.sbid)
        except: self.askap_schedblock = None
        
        ### get obsparams and obsvar
        if self.askap_schedblock is not None:
            self.obsparams = self.askap_schedblock.get_parameters()
            self.obsvar = self.askap_schedblock.get_variables()

    @property
    def antennas(self):
        ants = self.obsvar["schedblock.antennas"]
        ants = ants.replace("'", "").replace(" ", "")
        return ants[1:-1].split(",") # remove '' and split by comma

    @property
    def corrmode(self):
        """corrlator mode"""
        return self.obsparams["common.target.src%d.corrmode"]        
        
    @property
    def template(self, ):
        return self.askap_schedblock.template
      
    @property
    def spw(self, ):
        try:
            if self.template in ["OdcWeights", "Beamform"]:
                return eval(self.obsvar["schedblock.spectral_windows"])[0]
            return eval(self.obsvar["weights.spectral_windows"])[0]
        except: return [-1, -1]
        # note - schedblock.spectral_windows is the actual hardware measurement sets spw
        # i.e., for zoom mode observation, schedblock.spectral_windows one is narrower
    
    @property
    def central_freq(self, ):
        try: return eval(self.obsparams["common.target.src%d.sky_frequency"])
        except: return -1
        
    @property
    def footprint(self, ):
        return self.askap_schedblock.get_footprint_name()
    
    @property
    def status(self,):
        return self.askap_schedblock._service.getState(self.sbid)
        # return sbstatus.value, sbstatus.name

    @property
    def owner(self,):
        return self.askap_schedblock._service.getOwner(self.sbid)
    
    @property
    def alias(self, ):
        try: return self.askap_schedblock.alias
        except: return ""
    
    @property
    def start_time(self, ):
        try: return Time(self.obsvar["executive.start_time"]).mjd # in mjd
        except: return 0

    @property
    def sched_time(self, ):
        try: return Time(self.obsvar["scheduler.time"]).mjd # in mjd
        except: return 0

    @property
    def weight_sched(self, ):
        try: return int(self.obsvar["weights.schedulingblock"])
        except: return -1
    
    @property
    def duration(self, ):
        if self.status.value <= 3: return -1 # before execution
        try: return eval(self.obsvar["executive.duration"])
        except: return -1

    @property
    def fcm_version(self, ):
        try: return eval(self.obsvar["fcm.version"])
        except: return -1

### specific class for MWA triggering
### we need to use this class when a given SBID is executed/scheduled

class MWATriggerTSP:
    
    TRI_TS_ONFINISH = "~/test.py"
    TRI_RUN_TS_SOCKET = "/data/craco/craco/tmpdir/queues/mwatrigger"
    TMPDIR = "/data/craco/craco/tmpdir"

    def __init__(
            self, sbid, askap_project_ids=None, mwa_project_id="T001", 
            dryrun=True, **kwargs
        ):
        self.sbid = sbid
        self.askap_project_ids = askap_project_ids
        self.mwa_project_id = mwa_project_id
        self.dryrun = dryrun
        self.kwargs = kwargs

        logger.info(f"ASKAP SchedBlock ID - {self.sbid}, allowed ASKAP project IDs - {self.askap_project_ids}, MWA project ID - {self.mwa_project_id}")

        ### setup sbid checker...
        self.schedblock = ASKAPSchedBlock(sbid=sbid)

    def scheduled_run(self,):
        """
        this function is designed to be executed when this sbid is scheduled

        TODO
        - check the current status of MWA, oversampling?
        - trigger the change config observation?
        """
        pass
    
    def executing_run(self, ):
        """
        this function is designed to be executed when this sbid is being executed

        TODO
        - write a script for a tsp job
        - this function is used for triggering the tsp job...
        """
        ### check project...
        if self.schedblock.template in ["Beamform", "OdcWeights"]:
            logger.info(f"SB{self.sbid} is a {self.schedblock.template} scan... will to nothing")
            return

        if self.askap_project_ids is not None:
            if self.schedblock.owner not in self.askap_project_ids:
                logger.info(f"SB{self.sbid} is part of {self.schedblock.owner}... not in the allowed list...")
                return
        else:
            logger.info(f"no allowed projects specified... will trigger on all observations...")

        # perhaps TODO - if it is odc run, just ignore them...

        #####################
        environment = {
            "TS_SOCKET": self.TRI_RUN_TS_SOCKET,
            # "TS_ONFINISH": self.TRI_TS_ONFINISH,
            "TMPDIR": self.TMPDIR,
        }
        ecopy = os.environ.copy()
        ecopy.update(environment)

        cmd = f"""`which askap_trigger_mwa` -s {self.sbid} -p {self.mwa_project_id}"""
        if self.dryrun: cmd += " --dryrun"

        subprocess.run(
            [f"tsp {cmd}"], shell=True, capture_output=True,
            text=True, env=ecopy,
        )

### this is the class for updating things when a SB status has been changed
### you might want to have multiple classes here, they should all inherit from iceint.schedblock.ISBStateMonitor
class LotrunRunner(iceint.schedblock.ISBStateMonitor):

    ASKAP_MWA_TRIGGER_CONFIG = "./askap_trigger_config.json"

    def __init__(self, values=None):
        super().__init__()
        self.values = values

        self._load_trigger_config()

    def _load_trigger_config(self,):
        self.project_alias = self.values.project
        with open(self.ASKAP_MWA_TRIGGER_CONFIG) as fp:
            trigger_config = json.load(fp)
        if self.project_alias not in trigger_config:
            logger.info(f"configuration for project alias {self.project_alias} not in {self.ASKAP_MWA_TRIGGER_CONFIG}...")
            logger.info(f"proceed with default setup instead...")
            askap_mwa_pairs = trigger_config["default"]
        else:
            askap_mwa_pairs = trigger_config[self.project_alias]
            logger.info(f"loading setup for project alias {self.project_alias}...")

        self.askap_project_ids = askap_mwa_pairs["askap_project_ids"]
        self.mwa_project_id = askap_mwa_pairs["mwa_project_id"]
        logger.info(f"Observation from {self.askap_project_ids} will trigger observation for MWA project {self.mwa_project_id}...")
        
    def changed(self, sbid, state, updated, old_state, current=None):
        logger.info(f"ASKAP SB{sbid} status change from {old_state} to {state}")
        #########################################################
        if state == ObsState.EXECUTING:
            ### TODO - might specify project ids etc in a file...
            mwatriggertsp = MWATriggerTSP(
                sbid=sbid, askap_project_ids=self.askap_project_ids,
                mwa_project_id=self.mwa_project_id, dryrun=self.values.dryrun
            )
            mwatriggertsp.executing_run()
        
if __name__ == "__main__":
    ### this is for command line argument...
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Setup ASKAP Schedblock Listener', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-p", "--project", type=str, help="alias name for sbid mwa project pairs", default="default")
    parser.add_argument("--dryrun", action="store_true", help="whether run as a dry run or not", default=False)
    values = parser.parse_args()

    runner = LotrunRunner(values=values)
    state = SBStateSubscriber(runner)
    try:
        state.ice.waitForShutdown()
    except KeyboardInterrupt:
        state.topic.unsubscribe(state.subscriber)
        state.ice.shutdown()

