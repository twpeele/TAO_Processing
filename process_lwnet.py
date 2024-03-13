# ==========================
# File Name: process_lwnet.py
# Author: Toby Peele
# Date: 01/24/2024
# ==========================

import pandas as pd
import os
from datetime import datetime, timedelta

def process_lwnet(filename, outputPath, starttime, endtime):
    """
    Processes downloaded "lwnet" longwave radiation data file.

    Parameters:
        filename (String) - the name of the file to be processed
        outputPath (String) - the path of the location where the processed .csv files will be stored
        starttime (Datetime) - the start time
        endtime (Datetime) - the end time
    """

    df = parseLWnet(filename)
    pd.to_datetime(df['Datetime'])

    idx = filename.find('_')
    location = filename[5:idx]
    
    current = starttime

    while current <= endtime:

        stop = current + timedelta(days=1)

        tempdf = df.loc[(df['Datetime'] >= current) & (df['Datetime'] < stop)]
        if not tempdf.empty:
            tempdf.reset_index(inplace=True, drop=True)
            
            outputFile = outputPath + datetime.strftime(current, '%Y%m%d') + '_' + location + '_lwnet.csv'

            with open(outputFile, 'w') as f:
                for i in tempdf.index:
                    yr = str(tempdf['Datetime'][i].year)
                    tms = datetime.strftime(tempdf['Datetime'][i], '%Y-%m-%d %H:%M:%S')
                    f.write(location + ',' + 'netLW,' + yr + ',' + tms + ',0,' + str(tempdf['netLW'][i]) + ', ,1hr_TAO,0' + '\n')
            
            print('Created ' + outputFile)

        current = stop
    
    os.remove(filename)

def parseLWnet(filename):
    """
    Parses data from specified file into a dataframe.

    Parameters:
        filename (String) - the name of the file to be parsed
    Returns:
        df (Dataframe) - the newly created dataframe
    """

    headings = ['YYYYMMDD', 'HHMM', 'netLW', 'Q', 'S']

    df = pd.read_fwf(filename, names=headings, skiprows=5, na_values=[-999.99], parse_dates={'Datetime': ['YYYYMMDD', 'HHMM']})

    return df