import logging
import requests
import pandas as pd
def create_logger():
    logger = logging.getLogger("root")
    logger.setLevel(logging.DEBUG)
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger

def get_api_key(path) -> str:
    key_file = open(path, "r")
    key = key_file.readline().strip()
    return key

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
    # Set up your Geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)

    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()

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

import pickle
def write_results_to_pickle(results, filename):
    with open(filename, 'wb') as f:
        pickle.dump(results, f)

def read_results_from_pickle(filename):
    with open(filename, 'rb') as f:
        res = pickle.load(f)
    return res

from geocode.join import join_input_and_output, preprocess_raw_data_for_join
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
    ah = hash(address) + hash(date) + hash(price)
    return ah

def remove_duplicates(df):
    result = df.drop_duplicates()
    return result

def standardise_data(df):
    df1 = remove_duplicates(df)
    df2 = df1.assign(address = df.address.apply(lambda x : normalise_address(x)))
    df3 = df2.assign(unique_id = df.apply(lambda x :create_unique_identifier(x.address, x.date_of_sale, x.price), axis=1))
    df3['unique_id'] = df3.unique_id.astype(str)
    return df3
