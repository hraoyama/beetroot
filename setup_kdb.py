import os
import pandas as pd
import re
import gzip
import shutil
from datetime import datetime
from qpython import qconnection
import logging

SRC_DATA_DIR = 'D:/data/m_data/EUX/'  # where the original files live
TRADES_DIR = "I:/beetroot/trades/"  # where KDB trades will live  # "I:/testkdb/trades/" #
ORDERS_DIR = "I:/beetroot/orders/"  # where KDB orders will live "I:/testkdb/books/"  #
BOOKS_DIR = "I:/beetroot/books/"  # where KDB books will live "I:/testkdb/books/"  #
EXTRACT_DIR = "I:/beetroot/csv_data_from_py"  # where we temporarily extract files and delete them after processing
LOG_DIR = "D:/data/logs/"  # where we write the logs of the process
Q_PORT_NUMBER = 40000  # port number where you started a multithreaded Q (q -p 40000 -s 12)

logging.basicConfig(filename=os.path.join(LOG_DIR, f"upload_kdb_main_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log").replace("\\", "/"),
                    filemode='a',
                    level=logging.DEBUG)


# Our license does not allow us to start a v4 Q process remotely so this will be for later...
def start_remote_q(port_num=40000):
    # these 2 go to global once we have a working license
    Q_DIR = "D:/q/w64"  # where the q executable lives
    N_Q_THREADS = 12  # the number of threads you want Q to run in
    # port_num = 40000  # + int(month_pattern.replace("0", ""))
    logging.info(f'Starting Q Process on port {port_num} for Trades.')
    q_process = os.popen(os.path.join(Q_DIR, 'q').replace("\\", "/") + f' -p {port_num} -s {N_Q_THREADS}')
    # Popen([os.path.join(Q_DIR, 'q').replace("\\", "/"), f' -p {port_num}']) # -s {N_Q_THREADS}
    return q_process


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


def copy_and_extract(zipped_trade_filename, parent_dir, temp_dir):
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
    extraction_ok = os.path.isfile(extracted_file)
    if not extraction_ok:
        logging.error(f'Failed to find extracted file {extracted_file}')
    
    return full_input_filename, full_output_filename, extracted_file, copied_original, extraction_ok


def process_order_file(q, zipped_book_filename, kdb_dir=ORDERS_DIR, parent_dir=None, temp_dir=None):
    full_input_filename, full_output_filename, extracted_file, copied_original, extraction_ok = copy_and_extract(zipped_book_filename, parent_dir, temp_dir)
    if not extraction_ok:
        return False
    
    date_string = date_string_from_file_name(extracted_file)
    logging.info(f"Processing {extracted_file}")
    
    q(f'orders:("SDIIIFIFIIISFFF";enlist "|") 0: `:{extracted_file};')
    q('update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `orders;')
    q('delete from `orders where Qty=0; delete ISIN, TimeMM, Date, TimeSec, MarketTime, MarketTimeMM from `orders;')
    q(f'orders:`sym`time xcols orders;')
    q(f'.Q.dpft[`:{kdb_dir};{date_string};`sym;`orders];')
    q(f'delete orders from `.;')  # should be OK because of single threaded execution ??? check
    
    clean_up_files(copied_original, extracted_file, full_output_filename)


def process_book_file(q, zipped_book_filename, kdb_dir=BOOKS_DIR, parent_dir=None, temp_dir=None):
    full_input_filename, full_output_filename, extracted_file, copied_original, extraction_ok = copy_and_extract(zipped_book_filename, parent_dir, temp_dir)
    if not extraction_ok:
        return False
    
    date_string = date_string_from_file_name(extracted_file)
    logging.info(f"Processing {extracted_file}")
    
    q(f'books:("SDIIFFFFFFFFFFFFFFFFFFFF";enlist "|") 0: `:{extracted_file};')
    q('update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `books;')
    q('delete from `books where Bid_Px_Lev_0<=0;delete ISIN, TimeMM, Date, TimeSec, MarketTime from `books;')
    q(f'books:`sym`time xcols books;')
    q(f'.Q.dpft[`:{kdb_dir};{date_string};`sym;`books];')
    q(f'delete books from `.;')  # should be OK because of single threaded execution ??? check
    q(f'.Q.dpft[`:{kdb_dir};{date_string};`sym;`books];')
    q(f'delete books from `.;')  # should be OK because of single threaded execution ??? check
    
    clean_up_files(copied_original, extracted_file, full_output_filename)


def process_trade_file(q, zipped_trade_filename, kdb_dir=TRADES_DIR, parent_dir=None, temp_dir=None):
    full_input_filename, full_output_filename, extracted_file, copied_original, extraction_ok = copy_and_extract(zipped_trade_filename, parent_dir, temp_dir)
    if not extraction_ok:
        return False
    
    date_string = date_string_from_file_name(extracted_file)
    logging.info(f"Processing {extracted_file}")
    q(f'trades:("SDIIIIFI";enlist "|") 0: `:{extracted_file};')
    q('update sym:ISIN, time: (1000*TimeMM) + Date + `second$(60*60*TimeSec div 10000) + (60 * (TimeSec mod 10000) div 100) + (TimeSec mod 100) from `trades;')
    q('delete from `trades where Price=0;delete ISIN, TimeMM, Date, TimeSec, MarketTime from `trades;')
    q(f'trades:`sym`time`Price`Qty`Volume xcols trades;')
    q(f'.Q.dpft[`:{kdb_dir};{date_string};`sym;`trades];')
    q(f'delete trades from `.;')  # should be OK because of single threaded execution ??? check
    
    clean_up_files(copied_original, extracted_file, full_output_filename)


def process_pattern_fileBatch(toMatchPattern, process_function, q, data_path, temp_dir, kdb_dir):
    match_pattern = re.compile(toMatchPattern)
    
    # make sure the file patterns are mutually exclusive, else you will be loading multiple times the same data
    gz_csv_files = [x for x in os.listdir(data_path) if match_pattern.match(x) is not None]
    if not gz_csv_files:
        logging.error(f"No  files found for pattern {toMatchPattern} in directory: {data_path}")
        return False
    gz_csv_files.sort()
    logging.info(f"Found files for pattern {toMatchPattern} : {str(gz_csv_files)}")
    for zipped_filename in gz_csv_files:
        process_function(q, zipped_filename, parent_dir=data_path, temp_dir=temp_dir, kdb_dir=kdb_dir)


def main():
    # # global so shared across potential processes (READ ONLY)
    logging.info('Started')
    # from 201705 201902 => for if we want to try this multiprocessing
    starting_month_patterns = [x.strftime("%Y%m") for x in list(pd.date_range("2017-05-01", "2019-03-01", freq='M'))]
    
    data_path = SRC_DATA_DIR
    if not os.path.exists(data_path):
        logging.error(f"Failed to open directory for reading: {data_path}")
    else:
        port_num = Q_PORT_NUMBER
        q = qconnection.QConnection(host='localhost', port=port_num)  # initialize connection (make sure it is multi-threaded startup)
        q.open()
        logging.info('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
        
        for month_pattern in starting_month_patterns:
            process_pattern_fileBatch("^" + month_pattern + "[0-9]{2}_\S+_MKtrade.csv.gz$", process_trade_file, q, data_path=SRC_DATA_DIR, temp_dir=EXTRACT_DIR,
                                      kdb_dir=TRADES_DIR)
        logging.info(f'Finished loading trades from {data_path}')
        for month_pattern in starting_month_patterns:
            process_pattern_fileBatch("^" + month_pattern + "[0-9]{2}_\S+_Order.csv.gz$", process_order_file, q, data_path=SRC_DATA_DIR, temp_dir=EXTRACT_DIR,
                                      kdb_dir=ORDERS_DIR)
        logging.info(f'Finished loading orders from {data_path}')
        for month_pattern in starting_month_patterns:
            process_pattern_fileBatch("^" + month_pattern + "[0-9]{2}_\S+_Book.csv.gz$", process_book_file, q, data_path=SRC_DATA_DIR, temp_dir=EXTRACT_DIR,
                                      kdb_dir=BOOKS_DIR)
        logging.info(f'Finished loading books from {data_path}')
        
        q.close()
        logging.info(f'Q Connection. Is connected: {q.is_connected()}')
    
    logging.info('Finished')


if __name__ == '__main__':
    main()

# \l I:/testkdb/trades/;
# exec count[select from trades where date=2017.06.02];

## a mixture of licensing and pickling issues give us multiprocessing problems...
# ps = [Process(target=process_month_trades, args=(logger, month_pattern, data_path, temp_dir))
#       for month_pattern in starting_month_patterns]
# for p in ps:
#     p.start()
# for p in ps:
#     p.join()
