#!/usr/bin/python3

import numpy as np
import pandas as pd
import matplotlib.pylab as plt
from ftplib import FTP
import zipfile
from io import StringIO
import ipdb
import sys, os
import re


class DataManager:

    server_adress = "ftp-cdc.dwd.de"
    filepath = "/pub/CDC/observations_germany/climate/daily/kl/historical/"
    description_file = "KL_Tageswerte_Beschreibung_Stationen.txt" # hier stehen die Wetterstationen drin

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
            for n in zf.namelist():
                if re.search("produkt_klima_Tageswerte", n):
                    print("Found", n)
                    with zf.open(n, "r") as f:
                        data = pd.read_csv(f, sep=b"\s*;\s*")
                    return data
        except IOError:
            print("file not found")
            print("Trying to download")
            self._download(fname)
            return self.get_zipfile(fname)
                   
    def test(self):
        for line in self.lookup:
            print(line)
        
    def get_station_number(self, city_name):
        hits = []
        for line in self.lookup:
            if re.search(city_name, line, flags=re.I):
                hits.append((line.split()[0], line.split()[6]))
        if len(hits) > 1:
            for num, name in hits:
                print(name)
            raise NameError("Multiple hits. Specify your request")
        elif len(hits) == 0:
            raise NameError("No hit at all.")
        return hits.pop()
       
    def get_weather_data(self, station_id):
        # Check first if file has already been downloaded
        for f in os.listdir("."):
            if re.search("tageswerte", f) and station_id in re.findall("tageswerte_0*(\d+)", f)[0]:
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
                    if station_id == re.findall("tageswerte_0*(\d+)", line)[0]:
                        print("Found matching file:")
                        print(line)
                        fname = line.split()[-1]
                        self.get_file(fname)
        return self.get_zipfile(fname)
                        

def main(*args):
    dm = DataManager()
    num, name = dm.get_station_number("kröllwitz")
    x = dm.get_weather_data(num)
    ipdb.set_trace()

if __name__ == "__main__":
    main()



