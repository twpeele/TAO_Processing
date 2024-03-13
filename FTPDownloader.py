# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 10:41:26 2019

@author: twpeele
"""

# =============================================================================
# File Name:FTPDownloader.py
# Version: 1.00
# Author: Toby Peele
# Date: 07/23/2019
# Description:  This file defines a general purpose wrapper class for downloading
# files from FTP sites. 
# =============================================================================

import os
import time
import ftplib

from datetime import datetime
from ftplib import FTP, FTP_TLS
from ftplib import Error, error_perm, all_errors

APP_PATH = os.getcwd()
FILE_SEP = os.sep

class FTPDownloader:
    """
    Defines an FTP Downloader that downloads files using FTP protocol. 

    """
    def __init__(self):

        self.FTPconn = None
        self.FTPSite = None
        self.FTPConnectError = 'Unable to establish connection! Please check url and permissions.'
        self.FTPFileNotFoundError = 'The specified file was not found on the server.'
        self.FTP_TIME_OUT = 60


    def download(self, ftpwdir, saveDir, filename):
        """
        This function does the actual work of downloading a file from the remote ftp directory.  

        Parameters:
            ftpwdir (String) - the working directory on the ftp where our file lives.
            saveDir (String) - the directory where the downloaded file will be saved.
            filename (String) - the file name of the file we want to download.
        
        Returns: 
            (Boolean) - returns True if successful, False otherwise.
        """

        ftpc = self.FTPconn
            
        if not os.path.exists(saveDir):
            os.makedirs(saveDir)
            os.chdir(saveDir)
        else:
            os.chdir(saveDir)
        if os.path.exists(saveDir + filename):
            return True
        
        try:
            ftpc.cwd(ftpwdir)
            file = ftpc.nlst(filename)
            if file:
                fhandle = open(file[0], 'wb')
                print('Getting %s' %file[0])
                ftpc.retrbinary('RETR ' + file[0], fhandle.write)
                fhandle.close()
                print('Download successful...')
            else:
                # no such file found! return false
                print(self.FTPFileNotFoundError)
                return False
        except EOFError as fe:
            # We've probably been disconnected so just reconnect to server after short wait.
            print(fe)
            self.__logError(fe)
            time.sleep(5)
            self.connect(self.FTPSite)
            self.download(ftpwdir, saveDir, filename)
        except ConnectionResetError as e:
            print(e)
            self.__logError(e)
            time.sleep(5)
            self.connect(self.FTPSite)
            self.download(ftpwdir, saveDir, filename)
        except ftplib.all_errors as e:
            # anything else we have a more serious problem
            print(filename)
            print(e)
            self.__logError(e)
            return False

        return True
    

    def connect(self, site, user='anonymous', passwd='anonymous@yahoo.com'):
        """
        This function allows the user to connect to a given FTP site.
        
        Parameters:
            site (String) - The desired FTP site.
            user (String) - User name for site. (optional)
            passwd (String) - The password for the site. (optional)  
        Returns: 
            (Boolean) - True if successful, false otherwise.
        """
        
        # Attempt to establish a connection. 
        # Max attempts is 5 and connection times out in 30

        self.FTPSite = site
        attempts = 0
        while attempts < 5:
            try:
                ftpc = FTP_TLS(site, timeout=self.FTP_TIME_OUT)
                ftpc.login(user, passwd)
                ftpc.prot_p()
                self.FTPconn = ftpc
                return True
            except (Error, error_perm) as e:
                print(self.FTPConnectError)
            except Exception as e:
                print(e)
            attempts += 1
        # We ran out of attempts if the code gets here... Return False
        return False   


    def close(self):
        """
        This function closes the ftp connection to remote server.

        Example Usage: FTPDownloader.close()
        """
        self.FTPconn.quit()


    def __logError(self, e):
        """
        Writes file write error to program error log.

        Parameters:
            e (Exception) - The exception (with message) to be logged.
        """
        ef = open(APP_PATH + FILE_SEP + 'ftp_error_log.txt', 'a+')
        ef.write('%s ' %str(datetime.now()))
        ef.write('%s\n' %str(datetime.utcnow()))
        ef.write(str(e) + '\n')
        ef.close()
        
    