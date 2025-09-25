#!/usr/bin/python

"""
Basic idea for the triggering
- Trigger the observation when the observation is executed... (only at this time you will know the target position)
- Iteratively check whether the given sbid is observing, if so, schedule 5 mins observation
    Also check whether this observation is already 2 hours long
"""

from ASKAPTrigger.askaptrigger import ASKAPSchedBlock

import sqlite3
import urllib
import requests
import json
import time
import sys
import os

from astropy.time import Time
from datetime import datetime

os.makedirs("./log", exist_ok=True)

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# stream handler
fh = logging.handlers.RotatingFileHandler(f"./log/mwatrigger.log", maxBytes=1e8,backupCount=5, )
fh.setFormatter(formatter)
fh.setLevel(logging.INFO)
logger.addHandler(fh)


class MWATrigger:
    """
    this class is used for purely MWA triggering
    """
    MWA_TRIGGER_KEY_PATH = "~/.config/mwa_trigger_key.json"
    MWA_TRIGGER_ENDPOINT = "http://mro.mwa128t.org/trigger"
    MWA_TRIGGER_DEFAULT_PARAM = "./trigger_mwa_config.json"

    def __init__(self, trigtype="triggerobs", project_id=None, dryrun=True, **kwargs):
        """
        class for purely MWA triggering using MWA triggering api endpoint

        params
        ----------
        trigtype: str
            type of the triggering, accepted parameters are "triggerobs", "triggervcs", and "triggerbuffer"
        **kwargs: keyword arguments passed to api endpoint
        """
        self.trigtype = trigtype
        if project_id is None: 
            raise ValueError("project_id not found in the parameter list... please specify it and try again...")
        self.project_id = project_id
        self._load_default_params()
        self.params.update(kwargs)
        self.dryrun = dryrun

        ### this is used for updating secure_key from env
        self._load_secure_key()
        self.trigger_response_list = []

    def _load_default_params(self,):
        with open(self.MWA_TRIGGER_DEFAULT_PARAM) as fp:
            default_params = json.load(fp)
        if self.project_id in default_params:
            self.params = default_params[self.project_id]
        else:
            self.params = default_params["default"]
        self.params["project_id"] = self.project_id

    def update_default_params(self, **kwargs):
        logger.info(f"updating default trigger parameter - {list(kwargs)}...")
        self.params.update(kwargs)

    def _load_secure_key(self,):
        # load secure key from ~/.config/mwa_trigger_key.json
        if "secure_key" in self.params:
            logger.warning(f"secure_key found in the parameter list... please remove it and put it in {self.MWA_TRIGGER_KEY_PATH}")
        with open(os.path.expanduser(self.MWA_TRIGGER_KEY_PATH)) as fp:
            keys = json.load(fp)
        self.params.update({"secure_key": keys[self.params["project_id"]]})
        logger.info(f"get secure_key for project {self.params['project_id']} successfully...")

    def check_array_ready(self, obstime=60):
        """
        check whether mwa can be interrupted by the given project
        """
        checklink = f"http://mro.mwa128t.org/trigger/busy?project_id={self.project_id}&obstime={obstime}"
        try:
            response = requests.get(checklink)
            response.raise_for_status()
        except Exception as error:
            logger.error(f"cannot get correlator status... - {error}")
            return None
        
        busy = response.json()
        return not busy

    def check_corr_ready(self, ):
        """
        check whether correlator is in oversampling mode or critical sampling mode
        """
        checklink = "http://mro.mwa128t.org/trigger/cstate"
        try: 
            response = requests.get(checklink)
            response.raise_for_status()
        except Exception as error:
            logger.error(f"cannot get correlator status... - {error}")
            return None 
        
        healthy, oversampling = response.json()
        return not oversampling

    def trigger(self, storeresponse=True, **kwargs):
        trigger_data = self.params.copy()
        trigger_data.update(kwargs)

        # I have no idea why requests.post(url, json=data) does not work
        # I will form a query string instead...
        # querystr = "&".join([f"{k}={urllib.parse.quote(v)}" for k, v in trigger_data.items()])
        querylst = []
        for k, v in trigger_data.items():
            if isinstance(v, str): querylst.append(f"{k}={urllib.parse.quote(v)}")
            else: querylst.append(f"{k}={v}")
        querystr = "&".join(querylst)
        url = f"{self.MWA_TRIGGER_ENDPOINT}/{self.trigtype}?{querystr}"
        logger.info(f"trigger the observation with following url - {url}")
        if self.dryrun:
            logger.info(f"dryrun... will not create a trigger...")
            return None
        try:
            response = requests.post(url, )
            response.raise_for_status()
            ### if success is False, need to retrigger...
            response_json = response.json()
            ### save response to log folder...
            if storeresponse:
                trigger_id = response_json["trigger_id"]
                logfolder = f"./log/triggers"
                os.makedirs(logfolder, exist_ok=True)
                with open(f"{logfolder}/{trigger_id}.response.json", "w") as fp:
                    logger.info(f"dumping response json to {logfolder}/{trigger_id}.response.json")
                    json.dump(response_json, fp, indent=2)
            self.trigger_response_list.append(response_json)
            success = response_json["success"]
            if success: return response.json()
            logger.warning(f"trigger is not successful... please check...")
            return None
        except requests.exceptions.RequestException as error:
            logger.info(f"error triggering mwa telescope - {error}")
            return None
        
####### this is for the database
class MWATriggerDB:
    def __init__(self, dbfname="./trigger.db", ):
        self.dbfname = dbfname
        self._init_db()
        
    def _init_db(self):
        self.conn = sqlite3.connect(self.dbfname)
        cursor = self.conn.cursor()
        ### this is for observation table
        cursor.execute('''
CREATE TABLE IF NOT EXISTS mwatrigger (
    SBID INTEGER PRIMARY KEY,
    TIME REAL,
    groupid INTEGER,
    calobs INTEGER
)''')
        self.conn.commit()
        ### this is for calibration table
        cursor.execute('''
CREATE TABLE IF NOT EXISTS mwacal (
    calgroupid INTEGER PRIMARY KEY,
    time REAL
)''')
        self.conn.commit()
        
        cursor.close()

    def insert_record(self, recordlst=None, **kwargs):
        """
        insert a single record either with a list or tuple of single record,
        or pass them in as kwargs
        """
        if recordlst is None: 
            recordlst = self._convert_insert_kwargs(kwargs)
        if recordlst is None:
            return # do nothing if no recordlst or sbid provided
        ### now we can update the database...
        try:
            cursor = self.conn.cursor()
            logger.info(f"insert record with following value - {recordlst}")
            cursor.execute("""INSERT INTO mwatrigger (SBID, Time, groupid, calobs) 
VALUES (?, ?, ?, ?)""", recordlst)
            self.conn.commit()
            cursor.close()

        except Exception as error:
            logger.error(f"cannot insert thie record! - {error}")
    
    def _convert_insert_kwargs(self, argdict):
        """
        convert kwargs into recordlst
        """
        args = ["sbid", "time", "groupid", "calobs"]
        if "sbid" not in argdict: return None
        return [argdict.get(arg) for arg in args]

    def insert_cal_record(self, recordlst=None, **kwargs):
        """
        insert a single record into the calibration table
        """
        if recordlst is None: 
            recordlst = self._convert_insert_cal_kwargs(kwargs)
        if recordlst is None:
            return
        
        try:
            cursor = self.conn.cursor()
            logger.info(f"insert record with following value - {recordlst}")
            cursor.execute("""INSERT INTO mwacal (calgroupid, time) 
VALUES (?, ?)""", recordlst)
            self.conn.commit()
            cursor.close()

        except Exception as error:
            logger.error(f"cannot insert this record to mwacal! - {error}")

    def _convert_insert_cal_kwargs(self, argdict):
        """
        convert kwargs into recordlst
        """
        args = ["calgroupid", "time"]
        if "calgroupid" not in argdict: return None
        return [argdict.get(arg) for arg in args]
    
    def update_record(self, sbid, recordlst=None, **kwargs):
        if recordlst is None:
            updates, recordlst = self._convert_update_kwargs(sbid, kwargs)
        else:
            updates = ["time = ?", "groupid = ?", "calobs = ?"]
            recordlst.append(sbid) # add sbid...
        if recordlst is None: return # nothing happened...
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""UPDATE mwatrigger SET {", ".join(updates)}
WHERE SBID = ?""", recordlst)
            logger.info(f"update record for {sbid} with following values - {recordlst}")
            self.conn.commit()
            cursor.close()
        except Exception as error:
            logger.error(f"cannot update this record! - {error}")

    def _convert_update_kwargs(self, sbid, argdict):
        args = ["time", "groupid", "calobs"]
        updates = [f"{k} = ?" for k in args if k in argdict]
        if len(updates) == 0: return None, None
        values = [argdict[k] for k in args if k in argdict]
        values.append(sbid) # add sbid as we need to specify sbid = ? in the query
        return updates, values

    def query_record(self, sbid):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""SELECT * FROM mwatrigger WHERE SBID = ?""", (sbid,))
            record = cursor.fetchone()

            if record:
                _, time, groupid, calobs = record
                return dict(time=time, groupid=groupid, calobs=calobs)
            else:
                return None
        except Exception as error:
            logger.error(f"cannot query this record! - {error}")
    
    def query_cal_record(self, time, window=1/24):
        """
        function to query calibration record within given window
        return True if there is a calibration record
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""SELECT * FROM mwacal WHERE TIME > {time - window}""")
            record = cursor.fetchone()

            if record: return True
            return False
        except Exception as error:
            logger.error(f"cannot query this record from mwacal... - {error}")

    def close(self,):
        self.conn.close()
######### end of the database...

class ASKAPMWATrigger:
    """
    this class is used for triggering MWA based on ASKAP observation
    """
    def __init__(self, sbid, project_id=None, dryrun=True, ):
        self.sbid = sbid
        self.schedblock = ASKAPSchedBlock(sbid=self.sbid)
        self.dryrun = dryrun
        self.mwatrigger = MWATrigger(project_id=project_id, dryrun=dryrun)
        self.mwatriggerdb = MWATriggerDB(dbfname="trigger.db")

        ### initiate database...
        self._init_db_record()

    def _init_db_record(self, ):
        _query = self.mwatriggerdb.query_record(self.sbid)
        if _query is not None:
            self.groupid = _query["groupid"] # do not do insert if there is already a record...
            return
        time = self.schedblock.start_time
        self.mwatriggerdb.insert_record(sbid=self.sbid, time=time, groupid=None, calobs=False)
        self.groupid = None

    @property
    def sbid_status(self,):
        return self.schedblock.status.value
    
    @property
    def mwa_status(self,):
        # check whether the array can be interrupted
        array_ready = self.mwatrigger.check_array_ready()
        corr_ready = self.mwatrigger.check_corr_ready()
        logger.info(f"mwa array ready: {array_ready}; correlator ready: {corr_ready}")
        return array_ready and corr_ready
    
    def running(self,):
        """
        a parameter to check whether a given sbid is running
        """
        if self.schedblock.status.value == 3:
            return True # running now
        elif self.schedblock.status.value < 3:
            return None # to be scheduled
        return False

    def get_schedblock_source(self,):
        try:
            self.schedblock._refresh_schedblock() # refresh to get new obsparam and obsvar
            self.schedblock.get_sources_coord()
            srclst = self.schedblock.source_coord
            if len(srclst) == 1:
                self.coord = srclst[list(srclst)[0]]
            else:
                logger.warning(f"{len(srclst)} sources found... will proceed with the last scan...")
                maxscan = max(self.schedblock.scan_src_match.keys())
                scansrc = self.schedblock.scan_src_match[maxscan]
                logger.info(f"scan number {maxscan} source name {scansrc}...")
                self.coord = srclst[scansrc]
            logger.info(f"SB{self.sbid} is targeting {self.coord}...")

            self.mwatrigger.update_default_params(ra=self.coord[0], dec=self.coord[1])
        except Exception as error:
            logger.warning(f"cannot get antenna pointing for {self.sbid}...")
            logger.warning(f"error msg - {error}")
            self.coord = (None, None)

    ### parse trigger response
    def _get_trigger_obsids(self, response, ):
        if self.dryrun: return [self._get_current_gps_time()]
        if response is None: return [None]
        obsid_list = response["obsid_list"]
        if len(obsid_list) == 0: return [self._get_current_gps_time()]
        return obsid_list

    def _get_current_gps_time(self,):
        now = Time(datetime.now())
        return int(now.gps)

    def _get_current_mjd_time(self,):
        now = Time(datetime.now())
        return now.mjd
    
    def trigger_mwa(self, **kwargs):
        if "ra" not in self.mwatrigger.params:
            logger.info("no ra/dec information found... will not trigger any observation...")
            logger.info(f"please check whether SB{self.sbid} is a science observation - template: {self.schedblock.template}")
            return None
        field = self.schedblock.alias
        if field: kwargs.update(dict(obsname=field)) # update alias...
        if self.groupid: kwargs.update(dict(groupid=self.groupid))
        response = self.mwatrigger.trigger(**kwargs)
        if (response is not None or self.dryrun) and self.groupid is None:
            self.groupid = self._get_trigger_obsids(response)[0]
            self.mwatriggerdb.update_record(sbid=self.sbid, groupid=self.groupid)
        return response

    def trigger_mwa_cal(self, calexptime=120, calsearchwindow=1/24, **kwargs):
        """
        this is used for triggering a bandpass calibrator observation only
        """
        ### check whether there is already a calibration record within given window time (i.e., 6 hours)
        mjdnow = self._get_current_mjd_time()
        calexist = self.mwatriggerdb.query_cal_record(mjdnow, window=calsearchwindow)
        if calexist: # there is already a calibration - do nothing
            self.mwatriggerdb.update_record(sbid=self.sbid, calobs=True)
            return None 

        if "ra" not in self.mwatrigger.params:
            logger.info("no ra/dec information found... will use zenith for fake run for calibrator...")
            kwargs.update(dict(alt=89, az=0)) # use alt and az to do that...
        if self.groupid: kwargs.update(dict(groupid=self.groupid))

        ### TODO - check whether the parameter asked is correctly passed to calibration as well - it should, need real test
        kwargs.update(dict(
            calexptime=calexptime, calibrator=True, 
            exptime=8, nobs=1 # schedule a fake short observation for calibration...
        ))
        field = self.schedblock.alias
        if field: kwargs.update(dict(obsname=f"{field}_cal")) # update alias...
        response = self.mwatrigger.trigger(**kwargs)
        if response is not None or self.dryrun: # update the database if it is a dryrun...
            self.mwatriggerdb.update_record(sbid=self.sbid, calobs=True)
            if self.groupid is None:
                self.groupid = self._get_trigger_obsids(response)[0]
                self.mwatriggerdb.update_record(sbid=self.sbid, groupid=self.groupid)
            ### update calibration database
            self.mwatriggerdb.insert_cal_record(calgroupid=self._get_current_gps_time(), time=self._get_current_mjd_time())
        return response

    def run(self, buffertime=30, calfirst=True, calexptime=120, **kwargs):
        status = self.sbid_status
        logger.info(f"SB{self.sbid} current status - {status}...")

        if status > 3:
            logger.info(f"SB{self.sbid} has already finished... abort...")
            return
        
        trigger_status = self.mwatriggerdb.query_record(sbid=self.sbid)
        calstatus = trigger_status["calobs"]

        self.get_schedblock_source() # you need to get coordinate first...

        if calfirst and not calstatus:
            ### again you need to make sure array in a good mode to get the calibration...
            mwastatus = self.mwa_status
            logger.info(f"SB{self.sbid} current status - {status}...; MWA current status - {mwastatus}")
            while not mwastatus:
                logger.info(f"mwa is current unable to perform this observation...")
                time.sleep(5) # wait every 5s...
                mwastatus = self.mwa_status
                if self.sbid_status > 3:
                    logger.info("waited mwa for too long - observation has already finished...")
                    self.mwatriggerdb.close()
                    return None
            #######################
            logger.info("scheduling calibrator observation...")
            # note - we have already implement no cal logic in the below function
            # i.e., no cal trigger if there is already a calibration within 6 hours
            response = self.trigger_mwa_cal(calexptime=calexptime, **kwargs)
            if response is not None or self.dryrun:
                time.sleep(calexptime + 8) # wait for the calibrator observation to be finished...
        
        status = self.sbid_status
        mwastatus = self.mwa_status
        logger.info(f"SB{self.sbid} current status - {status}...; MWA current status - {mwastatus}")
        while status < 3 or (status == 3 and not mwastatus):
            # it will go into this while loop if
            # (1) this sbid has not been executed;
            # (2) this sbid is ongoing, but mwa is not ready for observation
            if status < 3:
                logger.info(f"SB{self.sbid} has not been executed...")
            else:
                logger.info(f"mwa array is not ready for observation...")
            time.sleep(10)
            status = self.sbid_status
            mwastatus = self.mwa_status

        exptime = self.mwatrigger.params.get("exptime")
        while status == 3:
            mwastatus = self.mwa_status
            if not mwastatus: # array is not ready...
                status = self.sbid_status
                time.sleep(5)
                continue

            self.get_schedblock_source()
            ### as there is calwindow parameter in trigger_mwa_cal, therefore I just add it here
            response = self.trigger_mwa_cal(calexptime=calexptime, **kwargs)
            if response is not None or self.dryrun:
                time.sleep(calexptime + 8)
            ### now trigger the observation itself...
            response = self.trigger_mwa(**kwargs)
            if response is not None or self.dryrun:
                time.sleep(exptime - buffertime)
            else:
                # something goes wrong - either trigger service not working or something else
                time.sleep(5) # stop for a while to check status...
            status = self.sbid_status
        logger.info(f"SB{self.sbid} observation finishes...")

        # trigger_status = self.mwatriggerdb.query_record(sbid=self.sbid)
        # calstatus = trigger_status["calobs"]
        ### trigger a calibration at the end no matter what
        logger.info("scheduling calibrator observation...")
        self.trigger_mwa_cal(calexptime=calexptime, **kwargs)
        logger.info(f"waiting for the observation to be finished... {calexptime}s...")
        time.sleep(calexptime) # wait for the calibrator observation to be finished...
        
        logger.info(f"MWA triggered observation done - SB{self.sbid}")
        self.mwatriggerdb.close()
        
if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Schedule MWA trigger observations based on ASKAP observation SBID', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--sbid", type=int, help="ASKAP schedule block ID", default=None)
    parser.add_argument("-p", "--pid", type=str, help="MWA project ID for the triggered observations", default="T001")
    parser.add_argument("--dryrun", action="store_true", help="whether run as a dry run or not", default=False)
    values = parser.parse_args()

    trigger = ASKAPMWATrigger(sbid=values.sbid, project_id=values.pid, dryrun=values.dryrun, )
    trigger.run()
