import os
import pandas as pd
import re
from multiprocessing import Process
import logging
import gzip
import shutil
from datetime import datetime
from qpython import qconnection
import multiprocessing
import logging
from logging.handlers import QueueHandler, QueueListener
import time

TRADES_DIR = "I:/beetroot/trades/"  # where KDB trades will live  # "I:/testkdb/trades/" #
BOOKS_DIR = "I:/beetroot/books/"  # where KDB books will live "I:/testkdb/books/"  #
EXTRACT_DIR = "I:/beetroot/csv_data_from_py"  # where we temporarily extract files and delete them after processing
LOG_DIR = "D:/data/logs/"  # where we write the logs of the process
Q_DIR = "D:/q/w64"  # where the q executable lives
N_Q_THREADS = 12  # the number of threads you want Q to run in


# a logger that handles logging from multiple processes
def logger_init():
    mp_q = multiprocessing.Queue()
    # this is the handler for all log records
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s - %(process)s - %(message)s"))
    
    # ql gets records from the queue and sends them to the handler
    ql = QueueListener(mp_q, handler)
    ql.start()
    logger = logging.getLogger()
    
    logger.setLevel(logging.DEBUG)
    # add the handler to the logger so records from this process are handled
    logger.addHandler(handler)
    
    return ql, mp_q


def clean_up_files(copied_original, extracted_file, full_output_filename):
    # if passed_check:
    os.remove(f'{extracted_file}')
    if os.path.isfile(extracted_file):
        logging.error(f'Failed to delete {extracted_file}')
    else:
        logging.info(f'Deleted {extracted_file}')
    if copied_original:
        os.remove(f'{full_output_filename}')
        if os.path.isfile(full_output_filename):
            logging.error(f'Failed to delete {full_output_filename}')
        else:
            logging.info(f'Deleted {full_output_filename}')


def date_string_from_file_name(filename):
    return datetime.strptime(os.path.basename(filename).split("_")[0], '%Y%m%d').strftime("%Y.%m.%d")


def process_book_file(q, zipped_book_filename, kdb_books_dir=BOOKS_DIR, parent_dir=None, temp_dir=None):
    full_input_filename = zipped_book_filename if parent_dir is None else os.path.join(parent_dir, zipped_book_filename).replace("\\", "/")
    
    # if no output dir is specified, file will be extracted in place
    full_output_filename = None if temp_dir is None else os.path.join(temp_dir, zipped_book_filename).replace("\\", "/")
    
    copied_original = False
    if full_output_filename is not None:
        logging.info(f"Copying {full_input_filename} to {full_output_filename}")
        shutil.copyfile(full_input_filename, full_output_filename)
        copied_original = os.path.isfile(full_output_filename)
    else:
        full_output_filename = full_input_filename
    
    logging.info(f"Unzipping {full_output_filename}")
    extracted_file = full_output_filename.replace(".gz", "")
    with gzip.open(full_output_filename, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if not os.path.isfile(extracted_file):
        logging.error(f'Failed to find extracted file {extracted_file}')
        return False
    
    date_string = date_string_from_file_name(extracted_file)
    logging.info(f"Processing {extracted_file}")
    
    q(f'books:("SDIIFFFFFFFFFFFFFFFFFFFF";enlist "|") 0: `:{extracted_file};')
    q('update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `books;')
    q('delete from `books where Bid_Px_Lev_0<=0;delete ISIN, TimeMM, Date, TimeSec, MarketTime from `books;')
    q(f'books:`sym`time xcols books;')
    q(f'.Q.dpft[`:{kdb_books_dir};{date_string};`sym;`books];')
    
    clean_up_files(copied_original, extracted_file, full_output_filename)


def process_trade_file(q, zipped_trade_filename, kdb_trades_dir=TRADES_DIR, parent_dir=None, temp_dir=None,
                       add_check=True):
    full_input_filename = zipped_trade_filename if parent_dir is None else os.path.join(parent_dir, zipped_trade_filename).replace("\\", "/")
    
    # if no output dir is specified, file will be extracted in place
    full_output_filename = None if temp_dir is None else os.path.join(temp_dir, zipped_trade_filename).replace("\\", "/")
    
    copied_original = False
    if full_output_filename is not None:
        logging.info(f"Copying {full_input_filename} to {full_output_filename}")
        shutil.copyfile(full_input_filename, full_output_filename)
        copied_original = os.path.isfile(full_output_filename)
    else:
        full_output_filename = full_input_filename
    
    logging.info(f"Unzipping {full_output_filename}")
    extracted_file = full_output_filename.replace(".gz", "")
    with gzip.open(full_output_filename, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if not os.path.isfile(extracted_file):
        logging.error(f'Failed to find extracted file {extracted_file}')
        return False
    
    date_string = date_string_from_file_name(extracted_file)
    logging.info(f"Processing {extracted_file}")
    q(f'trades:("SDIIIIFI";enlist "|") 0: `:{extracted_file};')
    q('update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `trades;')
    q('delete from `trades where Price=0;delete ISIN, TimeMM, Date, TimeSec, MarketTime from `trades;')
    q(f'trades:`sym`time`Price`Qty`Volume xcols trades;')
    q(f'.Q.dpft[`:{kdb_trades_dir};{date_string};`sym;`trades];')
    
    clean_up_files(copied_original, extracted_file, full_output_filename)
    
    # # ZZZZ: I cannot get this check to work because the q command "\l I:/testkdb/trades/;" is not getting passed on correctly; some windows/linux/string representation BS...
    # # PLEASE fix this if you figure it out
    # passed_check = True
    # if add_check:
    #     q(f'delete trades from `.;')
    #     # q('\\l I:\\\\testkdb\\\\trades;')
    #     q('\\l %s;' % kdb_trades_dir)
    #     count = q('{ count[select from trades where date=x] }', date_string)
    #     if count <= 0:
    #         logging.error(
    #             f"Failed to upload to KDB {full_input_filename} => {full_output_filename} => {extracted_file}")
    #         passed_check = False


def process_month_books(logger, month_pattern, data_path, temp_dir=EXTRACT_DIR):
    qh = QueueHandler(logger)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(qh)
    
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"upload_kdb_books_{month_pattern}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log").replace("\\", "/"))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    
    # let us start a q process for every month pattern
    port_num = 31000 + int(month_pattern.replace("0", ""))
    logging.info(f'{month_pattern} Starting Q Process on port {port_num}')
    
    try:
        q_process = os.popen(os.path.join(Q_DIR, 'q').replace("\\", "/") + f' -p {port_num}')
        time.sleep(1)
        # let us connect to that Q process
        q = qconnection.QConnection(host='localhost', port=port_num)
        # initialize connection (make sure it is multi-threaded startup)
        q.open()
        logging.info('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
        
        book_pattern = re.compile("^" + month_pattern + "[0-9]{2}_\S+_Book.csv.gz$")
        
        # make sure the file patterns are mutually exclusive, else you will be loading multiple times the same data
        book_gz_csv_files = [x for x in os.listdir(data_path) if book_pattern.match(x) is not None]
        if not book_gz_csv_files:
            logging.error(f"No Book files found  for pattern {month_pattern} in directory: {data_path}")
            return False
        book_gz_csv_files.sort()
        logging.info(f"Found book files {str(book_gz_csv_files)}")
        
        for zipped_book_filename in book_gz_csv_files:
            process_book_file(q, zipped_book_filename, parent_dir=data_path, temp_dir=temp_dir, kdb_books_dir=BOOKS_DIR)
    
    finally:
        # stop q process
        q(f'\\')
        logging.info(f'{month_pattern} stopped Q Process on port {port_num}.')
        # close connection
        q.close()
        logging.info(f'{month_pattern} Q Connection. Is connected: {q.is_connected()}')
        # q_process.close()


def process_month_trades(logger, month_pattern, data_path, temp_dir=EXTRACT_DIR):
    qh = QueueHandler(logger)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(qh)
    
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"upload_kdb_trades_{month_pattern}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log").replace("\\", "/"))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    
    # let us start a q process for every month pattern
    port_num = 40000 + int(month_pattern.replace("0", ""))
    logging.info('f{month_pattern} Starting Q Process on port {port_num}.')
    
    try:
        q_process = os.popen(os.path.join(Q_DIR, 'q').replace("\\", "/") + f' -p {port_num}')
        # Popen([os.path.join(Q_DIR, 'q').replace("\\", "/"), f' -p {port_num}']) # -s {N_Q_THREADS}
        time.sleep(1)
        
        # let us connect to that Q process
        q = qconnection.QConnection(host='localhost', port=port_num)
        # initialize connection (make sure it is multi-threaded startup)
        q.open()
        logging.info('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
        
        trade_pattern = re.compile("^" + month_pattern + "[0-9]{2}_\S+_MKtrade.csv.gz$")
        
        # make sure the file patterns are mutually exclusive, else you will be loading multiple times the same data
        trade_gz_csv_files = [x for x in os.listdir(data_path) if trade_pattern.match(x) is not None]
        if not trade_gz_csv_files:
            logging.error(f"No Trade files found  for pattern {month_pattern} in directory: {data_path}")
            return False
        trade_gz_csv_files.sort()
        logging.info(f"Found trade files {str(trade_gz_csv_files)}")
        for zipped_trade_filename in trade_gz_csv_files:
            process_trade_file(q, zipped_trade_filename, parent_dir=data_path, temp_dir=temp_dir, kdb_trades_dir=TRADES_DIR)
    finally:
        # close connection
        q.close()
        logging.info(f'{month_pattern} Q Connection. Is connected: {q.is_connected()}')
        q_process.close()
        logging.info(f'{month_pattern} stopped Q Process on port {port_num}.')


def main():
    logging.basicConfig(filename=os.path.join(LOG_DIR, f"upload_kdb_main_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log").replace("\\", "/"),
                        filemode='a',
                        level=logging.DEBUG)
    
    log_q_listener, logger = logger_init()
    
    # # global so shared across potential processes (READ ONLY)
    logging.info('Started')
    # from 201705 201902 => for if we want to try this multiprocessing
    starting_month_patterns = [x.strftime("%Y%m") for x in list(pd.date_range("2017-05-01", "2019-03-01", freq='M'))]
    root = 'D:/data/m_data/'
    dirs_to_load = ['EUX']  # , 'ETF', 'MTA']
    
    for dir_to_load in dirs_to_load:
        data_path = os.path.join(root, dir_to_load).replace("\\", "/")
        if not os.path.exists(data_path):
            logging.error(f"Failed to open directory for reading: {data_path}")
            continue
        else:
            # process_month_trades(month_pattern, data_path, logging, temp_dir="I:/testkdb/tempdir"):
            temp_dir = EXTRACT_DIR
            ps = [Process(target=process_month_trades, args=(logger, month_pattern, data_path, temp_dir))
                  for month_pattern in starting_month_patterns[:3]]
            for p in ps:
                p.start()
            for p in ps:
                p.join()
            logging.info(f'Finished loading trades from {data_path}')
            ps = [Process(target=process_month_books, args=(logger, month_pattern, data_path, temp_dir))
                  for month_pattern in starting_month_patterns[:3]]
            for p in ps:
                p.start()
            for p in ps:
                p.join()
            logging.info(f'Finished loading books from {data_path}')
    
    log_q_listener.stop()
    
    logging.info('Finished')


if __name__ == '__main__':
    main()

# \l I:/testkdb/trades/;
# exec count[select from trades where date=2017.06.02];
