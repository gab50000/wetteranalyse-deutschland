#!/usr/bin/env python3

from functools import wraps, partial
import logging
import os
import pathlib
import re
import zipfile
import ftplib
from ftplib import FTP

import fire
import ftputil
import pandas as pd


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


search = partial(re.search, flags=re.IGNORECASE)

def complete_filename(f):
    @wraps(f)
    def f_new(self, filename, *args, **kwargs):
        try:
            result = f(self, filename, *args, **kwargs)
        except ftputil.error.FTPIOError:
            logger.debug(f"Did not find {filename}")
            list_of_files = self.ls()
            # filter files which start with filename and are not directories
            possible_files = [f.split()[-1] for f in list_of_files
                              if f.split()[-1].startswith(filename) and f[0] != "d"]
            if len(possible_files) == 1:
                filename = possible_files[0]
                result = f(self, filename)
            else:
                raise ValueError("File not found")
        return result
    return f_new


class FTPBrowser:
    def __init__(self, address):
        self.address = address
        logger.info("Connecting to %s", self.address)
        self.ftp = ftputil.FTPHost(self.address, "anonymous", "")

    def __del__(self):
        logger.debug("Terminate FTP connection")
        self.ftp.close()

    def ls(self, dir_=""):
        return self.ftp.listdir(dir_)

    def find(self, dir_=".", *, show_files=True, show_dirs=True, name=None):
        result = []
        for root, dirs, files in self.ftp.walk(dir_):
            logger.debug("Go to root %s", root)
            if show_files:
                for f in files:
                    if name is None or search(name, f):
                        logger.info("Found file %s", f)
                        result.append(os.path.join(root, f))
            if show_dirs:
                for d in dirs:
                    if name is None or search(name, d):
                        logger.info("Found directory %s", d)
                        result.append(os.path.join(root, d))
        return "\n".join(result)

    def cd(self, dir_):
        self.ftp.chdir(dir_)
        return self

    @complete_filename
    def cat(self, filename, encoding="utf-8"):
        logger.debug("Reading file %s", filename)
        logger.debug("Use encoding %s", encoding)
        with self.ftp.open(filename, "r", encoding=encoding) as f:
            result = f.read()
        return result

    @complete_filename
    def download(self, filename):
        self.ftp.download(filename, filename)


class DataManager:
    """Manage data access to the FTP server of Deutscher Wetterdienst"""

    server_adress = "ftp-cdc.dwd.de"
    filepath = pathlib.Path("/pub/CDC/observations_germany/climate/daily/kl/historical")
    # hier stehen die Wetterstationen drin
    description_file = filepath / "KL_Tageswerte_Beschreibung_Stationen.txt"

    def __init__(self):
        self.browser = FTPBrowser(self.server_adress)
        #self.browser.cd(str(self.filepath))
        self.lookup = self.browser.cat(str(self.description_file),
                                       encoding="ISO-8859-1").splitlines()

    def get_file(self, fname):
        try:
            with open(fname, "r", encoding="ISO-8859-1") as f:
                lookup = f.readlines()
                return lookup
        except IOError:
            print("File {} for station code not found".format(fname))
            print("Trying to download")
            self.browser.download(fname)
            return self.get_file(fname)

    def get_zipfile(self, fname):
        try:
            zf = zipfile.ZipFile(fname)
            for fname in zf.namelist():
                if re.search("produkt_klima_Tageswerte", fname):
                    print("Found", fname)
                    with zf.open(fname, "r") as f:
                        data = pd.read_csv(f, sep=";", parse_dates=[1], encoding="utf-8")
                        data.columns = data.columns.str.strip()
                    return data
        except IOError:
            print("file not found")
            print("Trying to download")
            self.browser.download(fname)
            return self.get_zipfile(fname)

    def test(self):
        for line in self.lookup:
            print(line)

    def get_station_numbers(self, city_name):
        hits = []
        for line in self.lookup:
            if search(city_name, line):
                hits.append((line.split()[0], line.split()[6]))
        return hits

    def get_weather_data_daily(self, station_id):
        station_id = str(station_id)
        # Check first if file has already been downloaded
        for f in os.listdir(os.curdir):
            if re.search("tageswerte", f) and station_id in re.findall(r"tageswerte_0*(\d+)", f)[0]:
                print("Found matching file:")
                print(f)
                fname = f
        else:
            station_names = self.browser.ls()
            print("Looking for", station_id)
            for line in station_names:
                if re.search("tageswerte", line):
                    match = re.findall(r"tageswerte_0*(\d+)", line)
                    if match and station_id == match[0]:
                        print("Found matching file:")
                        print(line)
                        fname = line.split()[-1]
                        self.get_file(fname)
        return self.get_zipfile(fname)


def main():
    fire.Fire()