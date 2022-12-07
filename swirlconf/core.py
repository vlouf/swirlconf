"""
SWIRL Global configuration class.
"""
__authors__ = "Valentin Louf"
__contact__ = "valentin.louf@bom.gov.au"
__version__ = "1.0.0"
__date__ = "2022/12/07"

import os
import re
import glob
import json
import time
import datetime
import warnings
import configparser

from typing import List


class Swirl:
    def __init__(
        self, root_dir="/srv/data/swirl", etc_dir="/etc/opt/swirl/", cmss_dir="/srv/data/cmss-client", do_checks=True
    ) -> None:
        self.calib_path = os.path.join(root_dir, "calib")
        self.cmss_egress_path = os.path.join(cmss_dir, "swirl-egress")
        self.cmss_ingress_path = os.path.join(cmss_dir, "swirl-ingress")
        self.config_path = os.path.join(root_dir, "config")
        self.config_3dwinds_path = os.path.join(root_dir, "config", "3dwinds")
        self.dvad_path = os.path.join(root_dir, "dvad")
        self.diagnostics_path = os.path.join(root_dir, "diagnostics")        
        self.flow_path = os.path.join(root_dir, "flow")
        self.grids_path = os.path.join(root_dir, "grids")
        self.log_path = os.path.join(root_dir, "log")
        self.nowcast_path = os.path.join(root_dir, "nowcast")
        self.realtime_path = os.path.join(root_dir, "realtime")
        self.vols_path = os.path.join(root_dir, "vols")
        self.vvad_path = os.path.join(root_dir, "vvad")
        self.winds_path = os.path.join(root_dir, "winds")
        if do_checks:
            self.check_paths_exist()
        self.set_regions(etc_dir)
        self.set_ports(etc_dir)
        self.set_radar_site_info()
        # Web pages:
        self.html_dir = "/srv/web/swirl/www/html"

    def check_paths_exist(self):
        for k, v in self.__dict__.items():
            if "path" in k:
                if "cmss" in k:
                    continue
                if not os.path.exists(v):
                    raise FileNotFoundError(f"Directory {v} not found.")    

    def set_regions(self, etc_dir):
        fname = os.path.join(etc_dir, "regions.json")
        with open(fname) as fid:
            self.regions = json.load(fid)
        if len(self.regions) == 0:
            raise ValueError("Problem with the wind region configuration file.")

        self.rid_regions = {}
        for k, v in self.regions.items():
            for n in v:
                if n in self.rid_regions.keys():
                    warnings.warn(
                        f"Key {n} is present in 2 different regions. "
                        "It does not support radars overlapping region for now."
                    )
                self.rid_regions[n] = k

    def set_ports(self, etc_dir):
        try:
            fname = os.path.join(etc_dir, "postmaster.conf")
            config = configparser.ConfigParser()
            config.read(fname)
            self.port_manager = config.getint("manager", "service")
            self.port_flow_service = config.getint("flow", "service")
            self.port_flow_dispatcher = config.getint("flow", "dispatcher")
            self.port_nowcast_service = config.getint("nowcast", "service")
            self.port_winds_service = config.getint("winds", "service")
            self.port_diagnostics_service = config.getint("diagnostics", "service")
        except Exception:
            self.port_manager = 9900
            self.port_flow_dispatcher = 9920
            self.port_flow_service = 9921
            self.port_winds_service = 9931
            self.port_diagnostics_service = 9941
            self.port_nowcast_service = 9951

    def set_radar_site_info(self):
        import pandas as pd

        radar_fname = os.path.join(self.config_path, "radar_site_list.csv")
        self.radar_site_info = pd.read_csv(radar_fname)
        if len(self.radar_site_info) == 0:
            raise ValueError(f"Invalid radar configuration file: {radar_fname}. Exiting code.")
        return None

    def update_rids_in_region(
        self, region_name: str, radar_dtime: datetime.datetime, max_radar_downtime: int
    ) -> List[int]:
        """
        Check if any radar in the region is down and update the RID list.
        (Checked if the original VOL files came in).

        Parameter:
        ==========
        region_name: str
        radar_dtime: datetime.datetime

        Returns:
        ========
        valid_rids: List[int, ...]
        """
        get_time = lambda x: datetime.datetime.strptime(re.findall("[0-9]{8}_[0-9]{6}", x)[-1], "%Y%m%d_%H%M%S")

        regions_rids = self.regions[region_name]
        datestr = radar_dtime.strftime("%Y%m%d")
        valid_rids = []
        for r in regions_rids:
            path = os.path.join(self.vols_path, str(r), datestr)
            if not os.path.exists(path):
                print(f"No data for radar {r} existing today. Removing radar {r} from region.")
                continue

            flist = sorted(glob.glob(os.path.join(path, "*.*")))
            if len(flist) == 0:
                print(f"No file found for radar {r}. Removing radar {r} from region.")
                continue

            rtime = get_time(flist[-1])
            delta = radar_dtime - rtime
            if delta.total_seconds() > max_radar_downtime:
                print(f"No data for radar {r} for {delta}. Removing radar {r} from region.")
            else:
                valid_rids.append(r)

        if len(valid_rids) == 0:
            raise ValueError(f"No radar currently available for region {region_name}.")

        return valid_rids


class Chronos:
    """
    https://www.youtube.com/watch?v=QcHvzNBtlOw
    """

    def __init__(self, messg=None):
        self.messg = messg

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, ntype, value, traceback):
        self.time = time.time() - self.start
        if self.messg is not None:
            print(f"{self.messg} took {self.time:.2f}s.")
        else:
            print(f"Processed in {self.time:.2f}s.")
