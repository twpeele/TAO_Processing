# ==========================
# File Name: process_met.py
# Author: Toby Peele
# Date: 12/15/2023
# ==========================

import pandas as pd
import os
from datetime import datetime, timedelta

def process_met(filename, outputPath, starttime, endtime):
    """
    Processes downloaded "met" meteorological data file.

    Parameters:
        filename (String) - the name of the file to be processed
        outputPath (String) - the path of the location where the processed .csv files will be stored
        starttime (Datetime) - the start time
        endtime (Datetime) - the end time
    """

    df = parseMet(filename)
    pd.to_datetime(df['Datetime'])

    idx = filename.find('_')
    location = filename[3:idx]

    current = starttime

    while current <= endtime:

        stop = current + timedelta(days=1)

        tempdf = df.loc[(df['Datetime'] >= current) & (df['Datetime'] < stop)]
        if not tempdf.empty:
            tempdf.reset_index(inplace=True, drop=True)

            outputFile = outputPath + datetime.strftime(current, '%Y%m%d') + '_' + location + '_met.csv'

            with open(outputFile, 'w') as f:
                for i in tempdf.index:
                    yr = str(tempdf['Datetime'][i].year)
                    tms = datetime.strftime(tempdf['Datetime'][i], '%Y-%m-%d %H:%M:%S')
                    f.write(location + ',' + 'windSpeed,' + yr + ',' + tms + ',0,' + str(tempdf['WSPD'][i]) + ', ,1hr_TAO,0' + '\n')
                    correctedWindDir = (tempdf['WDIR'][i] + 180) % 360
                    f.write(location + ',' + 'windDir,' + yr + ',' + tms + ',0,' + str(correctedWindDir) + ', ,1hr_TAO,0' + '\n')
                    f.write(location + ',' + 'temperature,' + yr + ',' + tms + ',0,' + str(tempdf['AIRT'][i] + 273.15) + ', ,1hr_TAO,0' + '\n')
                    f.write(location + ',' + 'stationRH,' + yr + ',' + tms + ',0,' + str(tempdf['RH'][i]) + ', ,1hr_TAO,0' + '\n')
                    dewpoint = (tempdf['AIRT'][i] - ((100 - tempdf['RH'][i]) / 5)) + 273.15
                    f.write(location + ',' + 'dewpoint,' + yr + ',' + tms + ',0,' + str(dewpoint) + ', ,1hr_TAO,0' + '\n')
            
            print('Created ' + outputFile)

        current = stop
    
    os.remove(filename)

def parseMet(filename): 
    """
    Parses data from specified file into a dataframe.

    Parameters:
        filename (String) - the name of the file to be parsed
    Returns:
        df (Dataframe) - the newly created dataframe
    """

    headings = ['YYYYMMDD', 'HHMM', 'UWND', 'VWND', 'WSPD', 'WDIR', 'AIRT', 'SST', 'RH', 'QUALITY', 'SOURCE']
    widths = [9, 5, 7, 8, 7, 7, 6, 8, 7, 6, 6]

    df = pd.read_fwf(filename, widths=widths, names=headings, skiprows=6, parse_dates={'Datetime': ['YYYYMMDD', 'HHMM']}, na_values=[-99.9])
    df.drop(df[(df['AIRT'] > 60) | (df['AIRT'] < -90)].index, inplace=True, errors='ignore')

    return df
