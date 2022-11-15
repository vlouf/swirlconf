"""
SWIRL Global configuration class.
"""
__authors__ = "Valentin Louf"
__contact__ = "valentin.louf@bom.gov.au"
__version__ = "0.6.0"
__date__ = "2022/11/15"

import os
import re
import glob
import json
import time
import pickle
import asyncio
import datetime
import warnings
import traceback
import configparser

from functools import wraps
from typing import List, Tuple

import pyproj
import numpy as np
import pandas as pd


class Swirl():
    def __init__(self, root_dir="/srv/data/swirl", etc_dir="/etc/opt/swirl/", cmss_dir="/srv/data/cmss-client") -> None:
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
                if "cmss" in k:
                    continue
                if not os.path.exists(v):
                    raise FileNotFoundError(f"Directory {v} not found.")

    def compute_baricentre(self, region_rids: List[int]) -> Tuple[float, float, np.ndarray]:
        """
        Compute the barycentre (central point) of the multi-Doppler region

        Parameters:
        ===========
        radar_region: str
            Name of the multi-Doppler region.

        Returns:
        ========
        bar_lon: float
            Longitude of the barycentre of that region
        bar_lat: float
            Latitude of the barycentre of that region
        rids: np.ndarray
            List of radar ID sorted by Cartesian from the barycentre (closest first).
        """
        nl = len(region_rids)
        if nl < 2:
            raise ValueError(f"Not a multi-Doppler region. {region_rids}")

        latitudes = np.zeros(nl)
        longitudes = np.zeros(nl)
        cartesian_distance = np.zeros(nl)
        rids = np.zeros(nl, dtype=int)
        for idx, rid in enumerate(region_rids):
            latitudes[idx] = self.get_lat(rid)
            longitudes[idx] = self.get_lon(rid)
            rids[idx] = rid

        bar_lat = latitudes.sum() / len(latitudes)
        bar_lon = longitudes.sum() / len(longitudes)

        cartesian_distance = np.sqrt((latitudes - bar_lat) ** 2 + (longitudes - bar_lon) ** 2)
        return bar_lon, bar_lat, rids[np.argsort(cartesian_distance)]

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

    def get_distance_between_radars(self, r0: int, r1: int) -> float:
        """
        Compute the distance between 2 radars (in meters)

        Parameters:
        ===========
        r0: int
            Radar ID first radar.
        r1: int
            Radar ID second radar.

        Returns:
        ========
        distance: float
            Distance in m.
        """
        proj = pyproj.Proj(
            "+proj=aea +lat_1=-32.2 +lat_2=-35.2 +lon_0=151.209 +lat_0=-33.7008 +a=6378137 +b=6356752.31414 +units=m"
        )
        radar_site_info = self.radar_site_info
        x = np.zeros((2))
        y = np.zeros((2))
        for idx, rid in enumerate([r0, r1]):
            df = radar_site_info[radar_site_info.id == rid]
            lat = df.site_lat.values
            lon = df.site_lon.values
            try:
                x[idx], y[idx] = proj(lon, lat)
            except ValueError:
                raise ValueError(f"x:{x}, y:{y}, idx:{idx}.")

        distance = np.sqrt((x[1] - x[0]) ** 2 + (y[1] - y[0]) ** 2)
        return distance

    def update_rids_in_region(self, region_name: str, radar_dtime: datetime.datetime, max_radar_downtime: int) -> List[int]:
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


def buffer(func):
    """
    Decorator to catch and process error messages.
    """
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                rslt = await func(*args, **kwargs)
            except FileExistsError:
                return None
            except FileNotFoundError:
                print(f"Could not find all files.")
                return None
            except Exception:
                traceback.print_exc()
                return None
            return rslt

    else:

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                rslt = func(*args, **kwargs)
            except FileExistsError:
                return None
            except FileNotFoundError:
                print(f"Could not find all files.")
                return None
            except Exception:
                traceback.print_exc()
                return None
            return rslt

    return wrapper


async def decode_message(message):
    data = pickle.loads(message)
    for key in ["who", "what", "where", "when", "uid"]:
        try:
            _ = data[key]
        except KeyError:
            raise KeyError(f"Key: {key} not found in incoming message.")

    return data


async def dispatch_message(message: str, port: int) -> None:
    """
    Handle outgoing connection.
    Dispatch message (assuming that the message is a valid radar file name) to
    the valid.

    Parameters:
    ===========
    message: str
        Message to send to the live service.
    """
    try:
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(message)
        writer.close()
    except ConnectionRefusedError:
        print(f"Could not send message to port {port}.")
        traceback.print_exc()

    return None
