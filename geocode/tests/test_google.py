import pytest
from geocode_funcs import create_logger, get_api_key

def test_logger_is_created() -> None:
    logger = create_logger()
    assert logger is not None

def test_get_api_key() -> None:
    path = 'key.txt'
    apikey = get_api_key(path)
    assert isinstance(apikey, str)

from geocode_funcs import get_google_results, get_api_key, read_results_from_pickle
key = get_api_key('/home/richie/Dropbox/Code/Python/geocoder/key.txt')
def test_nonfull_results():
    kilcanway_nonfull = get_google_results("Kilcanway, Mallow, Co. Cork, Ireland",
                                           api_key = key,
                                           return_full_response = False)
    oldnon_full = read_results_from_pickle('nonfull_kilcanway.pkl')
    assert kilcanway_nonfull == oldnon_full

def test_full_results():
    kilcanway_full = get_google_results("Kilcanway, Mallow, Co. Cork, Ireland",
                                           api_key = key,
                                           return_full_response = True)
    old_full = read_results_from_pickle('full_kilcanway.pkl')
    assert kilcanway_full == old_full

import pandas as pd
from pathlib import Path
import os
from geocode_funcs import create_logger, log_progress_and_results
resource_path = "/home/richie/Dropbox/Code/Python/geocoder" 
output_data = pd.read_csv(os.path.join(resource_path, 'output_full_2018_19.csv'))
output_data_100 = output_data.iloc[0:100,]
output_data_500 = output_data.iloc[0:500,]
output_data_10k = output_data
output_filename = "test_output_file.csv"
logger = create_logger()


def test_output_data_exists():
    assert output_data is not None


def test_output_length_100(caplog) -> None:
    log_progress_and_results(output_data_100, logger, output_data_100.input_string,
                             output_filename)
    assert 'Completed 100 of 100 address' in caplog.text


def test_output_length_500() -> None:
    log_progress_and_results(output_data_500,
                             logger,
                             output_data_500.input_string,
                             output_filename)
    assert os.path.exists(output_filename + '_bak')

def test_output_length_10000() -> None:
    log_progress_and_results(output_data_10k,
                             logger,
                             output_data_10k.input_string,
                             output_filename)
    assert os.path.exists(output_filename + '.csv')
