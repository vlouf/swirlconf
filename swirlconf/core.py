"""
SWIRL Global configuration class.
"""
__authors__ = "Valentin Louf"
__contact__ = "valentin.louf@bom.gov.au"
__version__ = "0.5.1"
__date__ = "2021/11/17"

import os
import json
import time
import warnings
import configparser

import pandas as pd


class Swirl():
    def __init__(self, root_dir="/srv/data/swirl", etc_dir="/etc/opt/swirl/") -> None:
        # self.azshear_path = os.path.join(root_dir, "azshear")
        self.calib_path = os.path.join(root_dir, "calib")
        self.config_path = os.path.join(root_dir, "config")
        self.config_3dwinds_path = os.path.join(root_dir, "config", "3dwinds")
        self.dvad_path = os.path.join(root_dir, "dvad")
        self.diagnostics_path = os.path.join(root_dir, "diagnostics")
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
        self.set_radar_site_info()
        self.layered_conf = self._get_conf()

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
        self.port_diagnostics_service = config.getint("diagnostics", "service")
        self.port_diagnostics_dispatcher = config.getint("diagnostics", "dispatcher")

    def set_radar_site_info(self):
        radar_fname = os.path.join(self.config_path, "radar_site_list.csv")
        self.radar_site_info = pd.read_csv(radar_fname)
        if len(self.radar_site_info) == 0:
            raise ValueError(f"Invalid radar configuration file: {radar_fname}. Exiting code.")

    def set_switches(self, etc_dir):
        fname = os.path.join(etc_dir, "postmaster.conf")
        config = configparser.ConfigParser()
        config.read(fname)
        self.do_unravel = config.getboolean("unravel", "active")
        self.do_flow = config.getboolean("flow", "active")
        self.do_winds = config.getboolean("winds", "active")
        self.do_diagnostics = config.getboolean("diagnostics", "active")

    def get_lat(self, rid):
        """
        Get latitude for given radar ID.

        Parameter:
        ==========
        rid: int
            Radar Rapic ID

        Returns:
        ========
        latitude: float
            Radar site latitude
        """
        return self.radar_site_info.loc[self.radar_site_info.id == rid].site_lat.values[0]

    def get_lon(self, rid):
        """
        Get longitude for given radar ID.

        Parameter:
        ==========
        rid: int
            Radar Rapic ID

        Returns:
        ========
        longitude: float
            Radar site longitude
        """
        return self.radar_site_info.loc[self.radar_site_info.id == rid].site_lon.values[0]

    def _get_conf(self):
        txt = """
# grid size
size "301 301"

# top left coordinates
left_top "-150500 150500"

# grid resolution
cell_delta "1000 -1000"

# horizontal grid units
units m

# altitude of lowest layer (m)
altitude_base 0.0

# altitude step between layers (m)
altitude_step 500.0

# number of layers
layer_count 41

# radar moment to generate CAPPIs from
moment DBZH

# whether to output the cappis as well as flow fields
output_cappis true

# whether to output the flow magnitude and angle fields
output_polar true

# maximum distance from CAPPI altitude to use reflectivities
max_alt_dist 20000

# exponent for inverse distance weighting when interpolating between vertical levels (2 is a good default)
idw_pwr 2.0

# threshold out cappis to this minimum DBZ before tracking
min_dbz 20

# Matrix orientation
origin xy

# speckle filter: suppress pixels with less than this many non-zero neighbours (3x3)
speckle_min_neighbours 3

# speckle filter: number of times to apply speckle filter
speckle_iterations 3

# parameters for optical flow algorithm
optical_flow
{
  alpha 80
  gamma 7.0
  scales 100
  zfactor 0.5
  tol 0.005
  initer 3
  outiter 12
}
"""
        return txt


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
