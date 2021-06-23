"""
SWIRL Global configuration class.
"""
__authors__ = "Valentin Louf"
__contact__ = "valentin.louf@bom.gov.au"
__version__ = "0.5.0"
__date__ = "2021/06"

import os
import json
import warnings
import configparser


class Swirl():
    def __init__(self, root_dir="/srv/data/swirl", etc_dir="/etc/opt/swirl/") -> None:
        self.azshear_path = os.path.join(root_dir, "azshear")
        self.calib_path = os.path.join(root_dir, "calib")
        self.config_path = os.path.join(root_dir, "config")
        self.config_3dwinds_path = os.path.join(root_dir, "config", "3dwinds")
        self.dvad_path = os.path.join(root_dir, "dvad")
        self.flow_path = os.path.join(root_dir, "flow")
        self.grids_path = os.path.join(root_dir, "grids")
        self.log_path = os.path.join(root_dir, "log")
        self.vols_path = os.path.join(root_dir, "vols")
        self.vvad_path = os.path.join(root_dir, "vvad")
        self.winds_path = os.path.join(root_dir, "winds")
        self.check_paths_exist()
        self.set_regions(etc_dir)
        self.set_ports(etc_dir)
        self.set_switches(etc_dir)

    def check_paths_exist(self):
        for k, v in self.__dict__.items():
            if "path" in k:
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
        fname = os.path.join(etc_dir, "postmaster.conf")
        config = configparser.ConfigParser()
        config.read(fname)
        self.port_unravel_service = config.getint("unravel", "service")
        self.port_unravel_dispatcher = config.getint("unravel", "dispatcher")
        self.port_flow_service = config.getint("flow", "service")
        self.port_flow_dispatcher = config.getint("flow", "dispatcher")
        self.port_winds_service = config.getint("winds", "service")
        self.port_winds_dispatcher = config.getint("winds", "dispatcher")

    def set_switches(self, etc_dir):
        fname = os.path.join(etc_dir, "postmaster.conf")
        config = configparser.ConfigParser()
        config.read(fname)
        self.do_unravel = config.getboolean("unravel", "active")
        self.do_flow = config.getboolean("flow", "active")
        self.do_winds = config.getboolean("winds", "active")
