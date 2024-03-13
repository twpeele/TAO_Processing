# ==========================
# File Name: process_TAO.py
# Author: Toby Peele
# Date: 02/02/2024
# ==========================

import os
import gzip
import shutil
from FTPDownloader import FTPDownloader
from mysql import connector
from __config import Configuration
from glob import glob
from process_met import process_met
from process_bp import process_bp
from process_sst import process_sst
from process_lwnet import process_lwnet
from process_lwnetnc import process_lwnetnc
from process_rad import process_rad
from process_rain import process_rain
from process_swnet_nclw import process_swnet_nclw
from cleanFileData import cleanFileData


def process_TAO(savedir, stationList, starttime, endtime):
    """ 
    Processes TAO data and uploads to database.

    Parameters:
        savedir (String) - the directory to save downloaded files to.
        stationList (List) - the list of stations to be processed.
        starttime (Datetime) - the start time. 
        endtime (Datetime) - the end time.
    """

    for station in stationList:

        print('Processing %s' %station)

        filename = 'met' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_met(newFilename, savedir, starttime, endtime)

        filename = 'bp' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_bp(newFilename, savedir, starttime, endtime)

        filename = 'sst' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_sst(newFilename, savedir, starttime, endtime)

        filename = 'lwnet' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_lwnet(newFilename, savedir, starttime, endtime)

        filename = 'lwnetnc' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_lwnetnc(newFilename, savedir, starttime, endtime)

        filename = 'rad' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_rad(newFilename, savedir, starttime, endtime)

        filename = 'rain' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_rain(newFilename, savedir, starttime, endtime)

        filename = 'swnet_nclw' + station + '_hr.ascii.gz'
        download(filename, savedir)
        if os.path.exists(filename) and os.path.isfile(filename):
            newFilename = filename[:len(filename)-3]
            extractGzip(savedir + filename, savedir + newFilename)
            os.remove(savedir + filename)
            cleanFileData(newFilename)
            process_swnet_nclw(newFilename, savedir, starttime, endtime)

    upload(savedir, is_backfilling=True)


def extractGzip(inputf, outputf):
    """
    Extracts a stored file from .gz archive.

    Parameters:
        inputf (String) - the input file to be processed.
        outputf (String) - the name of the new file after processing.
    """
    try:
        with gzip.open(inputf, 'rb') as f_in, open(outputf, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            if not f_out._checkClosed():
                f_out.close()
    except Exception as e:
        print(e)

def unpack(filename, savedir):
    """
    Unpacks downloaded .gz file. 

    Parameters: 
        filename (String) - the file to be unpacked.
        savedir (String) - the location of the file to be unpacked.
    """
    if os.path.exists(savedir + filename):
        newfilename = filename.replace('.gz', '.ascii')
        extractGzip(savedir + filename, savedir + newfilename)
        os.remove(savedir + filename)

def download(filename, savedir):
    """
    Downloads specified file to save location.

    Parameters:
        filename (String) - the name of the file to be downloaded.
        savedir (String) - the location where the downloaded file is to be stored.
    """
    ftpcwd = '/high_resolution/ascii/hr/'
    print('Downloading %s' %filename)
    try:
        ftpc = FTPDownloader()
        ftpc.connect('ftp.pmel.noaa.gov', user='taopmelftp', passwd='G10b@LCh@Ng3')
        ftpc.download(ftpcwd, savedir, filename)
        ftpc.close()
    except(Exception) as e:
        print(e)

def upload(savedir, is_backfilling):
    """
    Uploads processed .csv files to the database.

    Parameters:
        savedir (String) - the location where the files are stored.
        is_backfilling (Boolean) - whether or not backfill channel is to be used. 
    """
    config = Configuration()
    RESET_STAGING_BACKFILL_ID = 'ResetAsosStagingBackfillId'
    RESET_STAGING_ID = 'ResetAsosStagingId'
    if is_backfilling:
        staging_table = 'asos_staging_backfill'
    else:
        staging_table = 'asos_staging'

    filelist = glob(savedir + '**/*.csv', recursive=True)
    filelist.sort()

    print('Starting Upload Process')
    print('Connecting to database')

    conn = connector.MySQLConnection(user=      config.db['user'],
                                     password=  config.db['password'],
                                     host=      config.db['host'],
                                     database=  'hindsight_asos',
                                     allow_local_infile = True)

    print('Connected!')

    for file in filelist:

        cursor = conn.cursor()

        # Check to see if table exists. 

        filedate = parseDateFromFilename(file)
        myTableName = 'asos_' + filedate
        tableExists = False

        fetch_asos_table_names = (
            "SHOW TABLES IN hindsight_asos;"
        )

        cursor.execute(fetch_asos_table_names)
        response = cursor.fetchall()
        for r in response:
            if r[0] == myTableName:
                tableExists = True
                break 

        if not tableExists:
            print('Creating new table.')
            cursor.callproc('hindsight_asos.CreateAsosArchiveTable', (myTableName,))

        try:
            conn.start_transaction()

            print('Clearing staging table.')
            clear_staging_table = (
                "DELETE FROM hindsight_asos." + staging_table + " WHERE `id` > 0;"
            )
            cursor.execute(clear_staging_table)
            cursor.callproc('hindsight_asos.%s' % RESET_STAGING_BACKFILL_ID if is_backfilling else RESET_STAGING_ID)
            print('Clear complete!')

            load_file_to_staging = (
                "LOAD DATA LOCAL INFILE %s "
                "REPLACE INTO TABLE hindsight_asos." + staging_table + " "
                "FIELDS TERMINATED BY ',' "
                "LINES TERMINATED BY '\n' "
                "(asos_id, var_name, year, time, altitude, @nv, qualitative_val, source, report_type)"
                "SET numeric_val = NULLIF(@nv, 'nan')"
            )

            print('Loading %s into staging table...' % file)
            cursor.execute(load_file_to_staging, (file,))

            copy_file_to_daily = (
                "INSERT INTO hindsight_asos." + myTableName + " "
                "(asos_id, var_id, year, time, altitude, numeric_val, qualitative_val, source, report_type) "
                    "SELECT "
                    "staging.asos_id, "
                    "av.id, "
                    "staging.year, "
                    "staging.time, "
                    "staging.altitude, "
                    "staging.numeric_val, "
                    "staging.qualitative_val, "
                    "staging.source, " 
                    "staging.report_type "
                "FROM hindsight_asos." + staging_table + " staging "
                    "INNER JOIN hindsight_asos.asos_variables av ON staging.var_name = av.id "
                "ON DUPLICATE KEY UPDATE "
                    "numeric_val = VALUES(numeric_val), "
                    "qualitative_val = VALUES(qualitative_val), "
                    "source = VALUES(source), "
                    "report_type = VALUES(report_type)"
            )

            print('Copying data from staging to %s.' % myTableName)
            cursor.execute(copy_file_to_daily)

            # Finish the transaction
            conn.commit()
            cursor.close()
            os.remove(file)

            print('Complete!')
            
        except connector.Error as e:
            print('There was an ERROR during the feed, rolling back: {}'.format(e))
            conn.rollback()

    print('Process complete!')
    conn.close()

def parseDateFromFilename(filename):
    """
    Parses date from file name. 

    Parameters: 
        filename (String) - The file name to be parsed. 
    """
    idx = filename.rfind('\\') + 1
    myDate = filename[idx:(idx+8)]

    return myDate