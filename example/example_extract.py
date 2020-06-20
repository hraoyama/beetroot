from enum import Enum
from functools import partial
import os
import pandas as pd
import numpy as np
import re
import multiprocessing
import qpython
import logging
import gzip
import shutil
from datetime import datetime
from qpython import qconnection

# global so shared across potential processes (READ ONLY)
logging.basicConfig(filename=f"D:/data/logs/example_extract_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log",
                    level=logging.INFO)
logging.info('Started')
q = qconnection.QConnection(host='localhost', port=40000)
# initialize connection
q.open()
logging.info('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))


def date_string_from_file_name(filename):
    return datetime.strptime(os.path.basename(filename).split("_")[0], '%Y%m%d').strftime("%Y.%m.%d")

def process_trade_file(zipped_trade_filename, parent_dir=None, temp_dir=None):
    
    full_input_filename = zipped_trade_filename if parent_dir is None else os.path.join(parent_dir,
                                                                                        zipped_trade_filename)
    # if no output dir is specified, file will be extracted in place
    full_output_filename = None if temp_dir is None else os.path.join(temp_dir.zipped_trade_filename)
    
    if full_output_filename is not None:
        logging.info(f"Copying {full_input_filename} to {full_output_filename}")
        shutil.copyfile(full_input_filename, full_output_filename)
    else:
        full_output_filename = full_input_filename
        
    logging.info(f"Unzipping {full_output_filename}")
    with gzip.open(full_output_filename, 'rb') as f_in:
        extracted_file = full_output_filename.replace("\\.gz", "")
        date_string = date_string_from_file_name(extracted_file)
        logging.info(f"Processing {extracted_file}")
        q.query(qconnection.MessageType.SYNC, f'tab_trade:("SDIIIIFI";enlist "|") 0: `:{extracted_file};')
        q.query(qconnection.MessageType.SYNC,
                'update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `tab_trade;')
        q.query(qconnection.MessageType.SYNC,
                'delete from `tab_trade where Price=0;delete ISIN, TimeMM, Date, TimeSec, MarketTime from `tab_trade;')
        q.query(qconnection.MessageType.SYNC, f'tab_trade:`sym`time`Price`Qty`Volume xcols tab;')
        q.query(qconnection.MessageType.SYNC, f'.Q.dpft[`:I:/testkdb/trades/;{date_string};`sym;`tab_trade];')
        
        # tab_trade:("SDIIIIFI";enlist "|") 0: `:I:/beetroot/csv_data_from_py/20170505_EUX_MKtrade.csv;
        # update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `tab_trade;
        # delete from `tab_trade where Price=0;
        # delete ISIN, TimeMM, Date, TimeSec, MarketTime from `tab_trade;
        # tab_trade:`sym`time`Price`Qty`Volume xcols tab;
        # .Q.dpft[`:I:/testkdb/trades/;2017.05.05;`sym;`tab_trade];
        # delete tab_trade from `.;
        # \l I:/testkdb/trades/2017.05.05/;

        with open('file.txt', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    pass


def main():
    # from 201705 201902 => for if we want to try this multiprocessing
    starting_month_patterns = [x.strftime("%Y%m") for x in list(pd.date_range("2017-05-01", "2019-03-01", freq='M'))]
    
    book_patterns_suffix = "[0-9]{2}_\S+_Book.csv.gz$" # [re.compile("^" + x + "[0-9]{2}_\S+_Book.csv.gz$") for x in starting_month_patterns]
    trade_patterns_suffix = "[0-9]{2}_\S+_MKtrade.csv.gz$" # [re.compile("^" + x + "[0-9]{2}_\S+_MKtrade.csv.gz$") for x in starting_month_patterns]
    
    qty_pattern = re.compile("^.*_Qty_Lev_[0-9]$")
    px_pattern = re.compile("^.*_Px_Lev_[0-9]$")
    
    root = 'D:/data/m_data/'
    dirs_to_load = ['EUX', 'ETF', 'MTA']
    
    n_processes = multiprocessing.cpu_count()
    book_gz_csv_files = []
    trade_gz_csv_files = []
    
    for dir_to_load in dirs_to_load:
        data_path = os.path.join(root, dir_to_load)
        if not os.path.exists(data_path):
            logging.error(f"Failed to open directory for reading: {data_path}")
            continue
        else:
            for month_pattern in starting_month_patterns:
                
                book_pattern = re.compile("^" + month_pattern + "[0-9]{2}_\S+_Book.csv.gz$")
                trade_pattern = re.compile("^" + month_pattern + "[0-9]{2}_\S+_MKtrade.csv.gz$")
                
                # make sure the file patterns are mutually exclusive, else you will be loading multiple times the same data
                book_gz_csv_files = [x for x in os.listdir(data_path) if book_pattern.match(x) is not None]
                trade_gz_csv_files = [x for x in os.listdir(data_path) if trade_pattern.match(x) is not None]
                
                if not book_gz_csv_files:
                    logging.error(f"No Book files found for pattern {month_pattern} in directory: {data_path}")
                if not trade_gz_csv_files:
                    logging.error(f"No Trade files found  for pattern {month_pattern} in directory: {data_path}")
                if not trade_gz_csv_files and not book_gz_csv_files:
                    continue

                logging.info(f"Found book files {str(book_gz_csv_files)}")
                logging.info(f"Found trade files {str(trade_gz_csv_files)}")
                
                book_gz_csv_files.sort()
                trade_gz_csv_files.sort()

                for zipped_trade_filename in trade_gz_csv_files:
                    process_trade_file(zipped_trade_filename, parent_dir=data_path, temp_dir="I:/testkdb/tempdir")
                
                # tag = dir_to_load.strip().upper()
                # func = partial(read_and_persist, data_path=data_path, qty_pattern=qty_pattern, px_pattern=px_pattern,
                #                tag=tag, lib=lib, action=arctic_action)
                
                # with multiprocessing.Pool(processes=n_processes) as pool:
                #    pool.map(func, book_gz_csv_files[:10])
                # for gz_csv_file in book_gz_csv_files:
                #     func(gz_csv_file)

    # close connection
    q.close()
    logging.info(f"Closed Q connection {q}." + 'IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))

    logging.info('Finished')


if __name__ == '__main__':
    main()

# tab_trade:("SDIIIIFI";enlist "|") 0: `:I:/beetroot/csv_data_from_py/20170505_EUX_MKtrade.csv;
# update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `tab_trade;
# delete from `tab_trade where Price=0;
# delete ISIN, TimeMM, Date, TimeSec, MarketTime from `tab_trade;
# tab_trade:`sym`time`Price`Qty`Volume xcols tab;
# .Q.dpft[`:I:/testkdb/trades/;2017.05.05;`sym;`tab_trade];
# delete tab_trade from `.;
# \l I:/testkdb/trades/2017.05.05/;
# \a;
# show[tab_trade]
#
# meta tab_trade
# select from tab_trade where Qty >10
# tab:("SDIIFFFFFFFFFFFFFFFFFFFF";enlist "|") 0: `:I:/beetroot/csv_data_from_py/20170505_EUX_Book.csv
# update TimeSpan: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `tab
