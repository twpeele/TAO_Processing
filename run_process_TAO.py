# ==============================
# File Name: run_process_TAO.py
# Author: Toby Peele
# Date: 03/12/24
# ==============================

import sys
from process_TAO import process_TAO
from datetime import datetime

def main(args):

    buoyFile = args[0]
    outputPath = args[1]
    starttime = args[2]
    endtime = args[3]

    start = datetime.strptime(starttime, '%Y%m%d_%H%M')
    end = datetime.strptime(endtime, '%Y%m%d_%H%M')

    buoys = list()

    with open(buoyFile, 'r') as f:
        for line in f:
            buoys.append(line.strip('\n'))

    process_TAO(outputPath, buoys, start, end)

if __name__ == "__main__":
    main(sys.argv[1:])