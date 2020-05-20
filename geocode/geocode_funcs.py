import logging
import requests

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
