#!/usr/bin/env python3

import logging
import os
import re
import zipfile
import ftplib
from ftplib import FTP

import fire
import pandas as pd


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def complete_filename(f):
    @wraps(f)
    def f_new(self, filename):
        try:
            result = f(self, filename)
        except ftplib.error_perm:
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
        self.ftp = FTP(self.address)
        self.ftp.login()
        logger.info(self.ftp.getwelcome())

    def __del__(self):
        logger.debug("Terminate FTP connection")
        self.ftp.close()

    def ls(self):
        result = []
        self.ftp.retrlines("LIST", result.append)
        return result

    def cd(self, dir_):
        self.ftp.cwd(dir_)
        return self

    def cat(self, filename):
        try:
            result = self._retrlines(f"RETR {filename}")
        except ftplib.error_perm:
            logger.debug(f"Did not find {filename}")
            list_of_files = self.ls()
            possible_files = [f.split()[-1] for f in list_of_files
                              if f.split()[-1].startswith(filename)]
            if len(possible_files) == 1:
                filename = possible_files[0]
                result = self.cat(filename)
            else:
                raise ValueError("File not found")
        return result

    def _retrlines(self, command):
        result = []
        logger.debug("Executing %s", command)
        self.ftp.retrlines(command, result.append)
        return result


class DataManager:

    server_adress = "ftp-cdc.dwd.de"
    filepath = "/pub/CDC/observations_germany/climate/daily/kl/historical/"
    # hier stehen die Wetterstationen drin
    description_file = "KL_Tageswerte_Beschreibung_Stationen.txt"
    def __init__(self):
        self.ftp = None
        self.lookup = self.get_file(self.description_file)

    def _connect_to_ftp(self):
        self.ftp = FTP(self.server_adress)
        self.ftp.login()

    def _download(self, fname):
        if not self.ftp:
            self._connect_to_ftp()
        self.ftp.cwd(self.filepath)
        with open(fname, "bw") as f:
            self.ftp.retrbinary("RETR "+fname, f.write)

    def get_file(self, fname):
        try:
            with open(fname, "r", encoding="ISO-8859-1") as f:
                lookup = f.readlines()
                return lookup
        except IOError:
            print("File {} for station code not found".format(fname))
            print("Trying to download")
            self._download(fname)
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
            self._download(fname)
            return self.get_zipfile(fname)

    def test(self):
        for line in self.lookup:
            print(line)

    def get_station_numbers(self, city_name):
        hits = []
        for line in self.lookup:
            if re.search(city_name, line, flags=re.I):
                hits.append((line.split()[0], line.split()[6]))
        return hits

    def get_weather_data(self, station_id):
        station_id = str(station_id)
        # Check first if file has already been downloaded
        for f in os.listdir(os.curdir):
            if re.search("tageswerte", f) and station_id in re.findall(r"tageswerte_0*(\d+)", f)[0]:
                print("Found matching file:")
                print(f)
                fname = f
        else:
            if not self.ftp:
                self._connect_to_ftp()
            self.ftp.cwd(self.filepath)
            station_names = []
            self.ftp.dir(station_names.append)
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
    fire.Fire(FTPBrowser)