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
