# ==================================
# File Name: cleanFileData.py
# Author: Toby Peele
# Date: 03/01/24
# ==================================


def cleanFileData(filename):
    """
    Cleans the headers the from a text file.

    Parameters:
        filename (String) - the file to be cleaned.
    """
    
    with open(filename, 'r') as f_in:
        content = f_in.readlines()

    with open(filename, 'w') as f_out:
        for line in content:
            sub = line[1:3]
            if sub.isnumeric():
                f_out.write(line)
    

                
