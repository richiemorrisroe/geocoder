import html
import datetime as dt

from geocoder.join import add_ireland_to_address
from geocoder.osm import geocode_addresses
from geocoder.geocode_funcs import create_logger, get_google_results, handle_ungeocodeable_address, join_input_and_output, NonGeoCodeableError
from geocoder.sql import append_to_table, check_for_new_rows, create_connection, format_results, get_data_from_db

def save_results(connection, results, original_data, logger, intersect_cols, geocoded_table):
    results_formatted = format_results(results)
    # logger.warning(f"{len(results_formatted)=}")
    logger.debug(f"{results_formatted.columns=}")
    results_final = join_input_and_output(original_data, results_formatted)
    results_final_s = results_final[list(intersect_cols)]
    results_final.to_parquet(f'gc_dub_full_{dt.datetime.now().isoformat()}.parquet')
    append_to_table(connection, table_name = geocoded_table, data=results_final_s)

def main():
    # NON_GC_TABLE = "property_sales_stg"
    NON_GC_TABLE = "prop_sales_dub_full_stg2"
    # NON_GC_TABLE = "prop_recent_test"
    GC_TABLE = "gc_google_results_shape_distinct"
    ## this matches the schema for what we have in our previous geocoded table
    intersect_cols = {'accuracy',
                      'address',
                      'date_of_sale',
                      'description_of_property',
                      'formatted_address',
                      'input_string',
                      'latitude',
                      'longitude',
                      'postcode',
                      'price',
                      'property_size_description',
                      'type',
                      'unique_id',
                      'vat_exclusive'}
    logger = create_logger()
    conn = create_connection("property.db")
    new_addresses = check_for_new_rows(conn, left_table=NON_GC_TABLE, right_table=GC_TABLE,
                                       county_name="Dublin", limit=93000, from_date='2019-05-23')
    try:
        addresses_to_skip = get_data_from_db(conn, "select * from non_geocodeable_addresses").address.unique().tolist()
    except Exception as e:
        addresses_to_skip = []
    logger.warning(f"{addresses_to_skip=}")
    new_add_full = add_ireland_to_address(new_addresses, "address")
    address_list = new_add_full.address.to_list()
    # breakpoint()
    address_list_accurate = [x for x in address_list if x not in addresses_to_skip]
    if len(address_list_accurate) == 0:
        raise ValueError("all results have been processed")
    logger.debug(f"{new_addresses.columns=}")

    logger.info(f"{new_add_full.columns=}")
    results = {}
    count = 0
    # breakpoint()
    try:
        for address in address_list_accurate:
            # breakpoint()
            if address in addresses_to_skip:
                print(f"{address=}")
                continue
            try:
                results[address] = get_google_results(html.unescape(address), api_key='AIzaSyDVyFETPq26N1ACgLmG_9hzdHqqvfXxItw')
                count += 1
                logger.warning(f"{count=}")
                if count % 10 == 0:
                    save_results(conn, results, new_add_full, logger, intersect_cols, GC_TABLE)
                    # logger.info(results)
                    # results_formatted = format_results(results)
                    # logger.warning(f"{len(results_formatted)=}")
                    # logger.debug(f"{results_formatted.columns=}")
                    # results_final = join_input_and_output(new_add_full, results_formatted)
                    # results_final_s = results_final[list(intersect_cols)]
                    # results_final.to_parquet('gc_dub_full.parquet')
                    # append_to_table(conn, table_name = GC_TABLE, data=results_final_s)
                    results = {}
            except NonGeoCodeableError as e:
                handle_ungeocodeable_address(conn, address)
                addresses_to_skip.append(address)
        
    except Exception as e:
        print(e)
        save_results(conn, results, new_add_full, logger, intersect_cols, GC_TABLE)
        # append_to_table(conn, table_name = GC_TABLE, data=results_final_s)
    finally:
        save_results(conn, results, new_add_full, logger, intersect_cols, GC_TABLE)


if __name__ == '__main__':
    res = main()
