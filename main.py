#!/usr/bin/python3

import numpy as np
#  import pandas as pd
#  import matplotlib.pylab as plt
from ftplib import FTP
import zipfile
from io import StringIO # um nichts auf die Festplatte zu schreiben, benutzen wir StringIO, mit dem wir ein file-artiges Objekt bekommen, das alles als String speichert.
import pdb
import sys
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
        with open(fname, "w") as f:
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
            self.get_file(fname)
        
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
       
    def get_weather_data(station_id):
        pass


def main(*args):
    dm = DataManager()
    num, name = dm.get_station_number("kröllwitz")
    pdb.set_trace()
 

if __name__ == "__main__":
    main()



