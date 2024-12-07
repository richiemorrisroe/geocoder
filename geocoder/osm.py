import json

import requests
import pandas as pd
from geocode.geocode_funcs import normalise_address


from geocode.join import add_ireland_to_address

from geocode.sql import check_for_new_rows
BASE_URL = "http://localhost:8080/search?"

def get_geocode_from_address(address, base_url=BASE_URL):
    query_params = {'q':address}
    result = requests.get(base_url, params=query_params)
    return result

def convert_response_to_json(result):
    content = result.content
    result = json.loads(content)
    result_dict = result[0]
    return result_dict


def geocode_addresses(connection, address_column:str, limit=10, logger=None):
    new_addresses = check_for_new_rows(connection, table_name="property_sales_stg",
                                       county_name="Dublin", limit=limit)

    new_addresses_full = add_ireland_to_address(new_addresses, "address")
    result_list = []
    for index, address in new_addresses_full.iterrows():
        add = address[address_column]
        result = get_geocode_from_address(address=add)
        if len(eval(result.content))==0:
            #we didn't get a match
            log_str = f"did not match {add}"
            if logger:
                logger.log(msg=log_str, level=10)
            else:
                print(log_str)
            res_dict = {}
        else:
            res_dict = convert_response_to_json(result)
        result_list.append({'address': add, 'data':res_dict})
    result_df = pd.DataFrame.from_records(result_list)
    return result_df
        
