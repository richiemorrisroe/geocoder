import logging
import pickle
import re
import sys

from geocoder.sql import append_to_table, load_data_into_table
import pandas as pd
import requests
from requests.utils import quote


from geocoder.join import join_input_and_output, preprocess_raw_data_for_join

class NonGeoCodeableError(Exception):
    pass

def create_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename="gc_logs.txt", level=logging.DEBUG)
    # create console handler
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.WARNING)
    logger.addHandler(ch)
    return logger

def get_api_key(path) -> str:
    key_file = open(path, "r")
    key = key_file.readline().strip()
    return key


def handle_ungeocodeable_address(conn, data):
    tbl_name = "non_geocodeable_addresses"
    print(f"{data=}")
    ##man i f'ing hate pandas, wtf is this bullshit?
    data_df = pd.DataFrame.from_dict({'address' : [data]})
    load_data_into_table(conn, table_name=tbl_name, data = data_df, if_exists="append")
    return 0
    
def get_google_results(address, api_key=None, return_full_response=False):
    """Get geocode results from Google Maps Geocoding API.
    Note, that in the case of multiple google geocode reuslts, 
    this function returns details of the FIRST result.
    @param address: String address as accurate as possible. For
    Example "18 Grafton Street, Dublin, Ireland" 
    @param api_key:
    String API key for Google Maps Platform
    @param
    return_full_response: Boolean to indicate if you'd like to return
    the full response from google. This is useful if you'd like
    additional location details for storage or parsing later.

    """
    logger = create_logger()
    # Set up your Geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(quote(address))
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)
    logger.debug(f"{geocode_url=}")

    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()
    logger.debug(f"{results=}")
    if results['status'] == 'REQUEST_DENIED':
        raise ValueError("request denied")
    # if there's no results or an error, return empty results.
    if len(results['results']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
        logger.warning(f"unable to geocode {address}")
        raise NonGeoCodeableError(f"unable to geocode {address}")

    else:    
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components') 
                                  if 'postal_code' in x.get('types')])
        }

    # Append some other details:    
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True:
        output['response'] = results

    return output




def write_results_to_pickle(results, filename):
    with open(filename, 'wb') as f:
        pickle.dump(results, f)

def read_results_from_pickle(filename):
    with open(filename, 'rb') as f:
        res = pickle.load(f)
    return res




def log_progress_and_results(results, logger, addresses, output_filename, input_data) -> None:
    # if len(results) % 6 == 0:
    #       results_df = pd.DataFrame(results)
    #       results_df_joined = join_input_and_output(input_data, results_df)
    #       results_df_joined.to_csv("{}_bak".format(output_filename))
    #       print("saved {r} results to file".format(r=len(results)))
    input_data = preprocess_raw_data_for_join(input_data, 'address')
    if len(results) % 100 == 0:
        logger.info("Completed {} of {} address".format(len(results), len(addresses)))
    if len(results) % 500 == 0:
        results_df = pd.DataFrame(results)
        results_df_joined = join_input_and_output(input_data, results_df)
        results_df_joined.to_csv("{}_bak".format(output_filename))
        print("saved {r} results to file".format(r=len(results)))
    if len(results) % 10000 == 0:
        results_df = pd.DataFrame(results)
        results_df_joined = join_input_and_output(input_data, results_df)
        results_df_joined.to_csv(output_filename)
        print("saved {r} results to file".format(r=len(results)))
        pd.DataFrame(results).to_csv(output_filename, encoding='utf8')


def normalise_address(address=None):
    if not address:
        raise ValueError("address must be supplied")
    else:
        lc_address = address.lower()
        
    return lc_address

def create_unique_identifier(address, date, price):
    ah = address + str(date) + str(price)
    return ah

def remove_duplicates(df):
    result = df.drop_duplicates()
    return result

def standardise_data(df, address_column):
    df1 = remove_duplicates(df)
    df2 = df1.assign(
                     date_of_sale=pd.to_datetime(df.date_of_sale, format='%Y-%m-%d'))
    df3 = df2.assign(unique_id = df.apply(lambda x :create_unique_identifier(normalise_address(x.address), x.date_of_sale, x.price), axis=1))
    df3['unique_id'] = df3.unique_id.apply(lambda x: re.sub('[^A-Za-z0-9]+', '', x)).astype(str)

    return df3
