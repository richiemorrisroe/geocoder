import pandas as pd
import logging
import time
from geocode.geocode_funcs import (create_logger, get_api_key, get_google_results,
                                   log_progress_and_results)
from geocode.join import preprocess_raw_data_for_join, join_input_and_output
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--backoff_time", type=int, help="backoff time", default=30)
parser.add_argument("--input_file", type=str, help="input file containing addresses", default=None)
parser.add_argument("--output_file", type=str, help="file to output geocoded results too", default=None)
parser.add_argument("--address_column", type=str, help="column name with addresses", default=None)
parser.add_argument("--return_full_results", type=bool, help="return full results?", default=True)
args = parser.parse_args()
logger = create_logger()

#------------------ CONFIGURATION -------------------------------

# Set your Google API key here. 
# Even if using the free 2500 queries a day, its worth getting an API key since the rate limit is 50 / second.
# With API_KEY = None, you will run into a 2 second delay every 10 requests or so.
# With a "Google Maps Geocoding API" key from https://console.developers.google.com/apis/, 
# the daily limit will be 2500, but at a much faster rate.
# Example: API_KEY = 'AIzaSyC9azed9tLdjpZNjg2_kVePWvMIBq154eA'
# key_file = open("key.txt", "r")
# key = key_file.readline().strip()
key = get_api_key("key.txt")
API_KEY = key

BACKOFF_TIME = args.backoff_time
# Set your output file name here.
output_filename = args.output_file
# Set your input file here
input_filename = args.input_file
# Specify the column name in your input data that contains addresses here
address_column_name = args.address_column
# Return Full Google Results? If True, full JSON results from Google are included in output
RETURN_FULL_RESULTS = args.return_full_results

#------------------ DATA LOADING --------------------------------

# Read the data to a Pandas Dataframe
data = pd.read_csv(input_filename, encoding='utf8')
if address_column_name not in data.columns:
        raise ValueError("Missing Address column in input data")

## preprocess data for join to output
data_pp = preprocess_raw_data_for_join(data, address_column_name)

# Form a list of addresses for geocoding:
# Make a big list of all of the addresses to be processed.
addresses = data[address_column_name].tolist()

# **** DEMO DATA / IRELAND SPECIFIC! ****
# We know that these addresses are in Ireland, and there's a column for county, so add this for accuracy. 
# (remove this line / alter for your own dataset)
addresses = (data[address_column_name] + ',' + data['county'] + ',Ireland').tolist()


#------------------ PROCESSING LOOP -----------------------------

# Ensure, before we start, that the API key is ok/valid, and internet access is ok
test_result = get_google_results("London, England", API_KEY, RETURN_FULL_RESULTS)
if (test_result['status'] != 'OK') or (test_result['formatted_address'] != 'London, UK'):
    logger.warning("There was an error when testing the Google Geocoder.")
    raise ConnectionError('Problem with test results from Google Geocode - check your API key and internet connection.')

# Create a list to hold results
results = []
# Go through each address in turn
for address in addresses:
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address with google
        try:
            geocode_result = get_google_results(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True

        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes
            geocoded = False
        else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            results.append(geocode_result)           
            geocoded = True

    #Print status every 100 addresses
    log_progress_and_results(results, logger, addresses, output_filename, data_pp)
    # if len(results) % 100 == 0:
    #     logger.info("Completed {} of {} address".format(len(results), len(addresses)))

    # # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    # if len(results) % 500 == 0:
    #     pd.DataFrame(results).to_csv("{}_bak".format(output_filename))
    #     print("saved {r} results to file".format(r=len(results)))
    # if len(results) % 10000 == 0:
    #         pd.DataFrame(results).to_csv(output_filename, encoding='utf8')
#All done
logger.info("Finished geocoding all addresses")
# Write the full results to csv using the pandas library.
pd.DataFrame(results).to_csv(output_filename, encoding='utf8')
