import os
from pathlib import Path

import pytest
import spatialite
import geopandas
import pandas as pd

from geocode.geocode_funcs import (create_logger, get_api_key,
                                   get_google_results, get_api_key,
                                   read_results_from_pickle, normalise_address, hash_address)
from geocode.sql import create_schema, create_table, create_connection, get_data_from_db, load_data_into_table, load_shapefile, get_property_data

from geocode.geocode_funcs import create_logger, log_progress_and_results
from geocode.join import join_input_and_output, preprocess_raw_data_for_join, add_ireland_to_address


@pytest.fixture
def property_data():
    result = pd.read_feather("../ppr_processed.feather")
    result_sample = result.sample(frac=0.01)
    return result_sample

@pytest.fixture
def geocoded_data():
    result = pd.read_csv("../../ppr_geocoded_till_oct2018.csv", encoding="latin1")
    result_sample = result.sample(frac=0.01)
    return result_sample

@pytest.fixture
def shapefile_path():
    return "../../electoral_divisions_gps.shp"

@pytest.fixture
def connection():
    return create_connection('property.db')
    

def test_logger_is_created() -> None:
    logger = create_logger()
    assert logger is not None

def test_get_api_key() -> None:
    path = 'key.txt'
    apikey = get_api_key(path)
    assert isinstance(apikey, str)

key = get_api_key('key.txt')
# def test_nonfull_results():
#     kilcanway_nonfull = get_google_results("Kilcanway, Mallow, Co. Cork, Ireland",
#                                            api_key = key,
#                                            return_full_response = False)
#     oldnon_full = read_results_from_pickle('nonfull_kilcanway.pkl')
#     assert kilcanway_nonfull == oldnon_full

# def test_full_results():
#     kilcanway_full = get_google_results("Kilcanway, Mallow, Co. Cork, Ireland",
#                                            api_key = key,
#                                            return_full_response = True)
#     old_full = read_results_from_pickle('full_kilcanway.pkl')
#     assert kilcanway_full == old_full


resource_path = "/home/richie/Dropbox/for_dropbox/geocoder/" 
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
                             output_filename, input_data_sample)
    assert 'Completed 100 of 100 address' in caplog.text


def test_output_length_500() -> None:
    log_progress_and_results(output_data_500,
                             logger,
                             output_data_500.input_string,
                             output_filename, input_data_sample)
    assert os.path.exists(output_filename + '_bak')

def test_output_length_10000() -> None:
    log_progress_and_results(output_data_10k,
                             logger,
                             output_data_10k.input_string,
                             output_filename,
                             input_data_sample)
    assert os.path.exists(output_filename)





input_data_sample = pd.read_csv("input_sample_data_one.csv")
output_data_sample = pd.read_csv("output_sample_data_one.csv")
input_data_sample5 = pd.read_csv("input_sample_data.csv")
output_data_sample5 = pd.read_csv("output_sample_data.csv")

def test_add_ireland_to_address():
    result = add_ireland_to_address(input_data_sample, "address")
    assert result[0].endswith("Ireland")


def test_join_input_and_output_one_row():
    input_data_pp = preprocess_raw_data_for_join(input_data_sample, "address")
    result_joined = join_input_and_output(input_data_pp, output_data_sample)
    assert result_joined.shape[0] == input_data_sample.shape[0]
    assert result_joined.shape[1] > input_data_sample.shape[1]

def test_join_input_and_output_five_rows():
    input_data_pp = preprocess_raw_data_for_join(input_data_sample5, "address")
    result_joined = join_input_and_output(input_data_pp, output_data_sample5)
    assert result_joined.shape[0] == input_data_sample5.shape[0]
    assert result_joined.shape[1] > input_data_sample5.shape[1]

def test_addresses_can_be_normalised():
    address = "17 Castleknock Brook, Castleknock, Dublin, Ireland"
    normalised_address = normalise_address(address)
    print(normalised_address)
    assert normalised_address.islower()

def test_address_can_be_hashed():
    address1 = "17 Castleknock Brook, Castleknock, Dublin, Ireland"
    address2 = "17 CASTLEKNOCK BROOK, CASTLEKNOCK, DUBLIN, IRELAND"
    address_hash1 = hash_address(normalise_address(address1))
    print(address_hash1)
    address_hash2 = hash_address(normalise_address(address2))
    assert address_hash1 == address_hash2


def test_sqlite_can_be_loaded(connection):
    import spatialite
    con = connection
    assert con.cursor() is not None

def test_sqlite_can_create_table(connection):
    import spatialite
    con = connection
    cursor = con.cursor()
    res = cursor.execute("CREATE TABLE IF NOT EXISTS property_sales_sample (year, price)")
    assert res.fetchone() is None

    
def test_processed_ppr_can_be_loaded(property_data):
    assert isinstance(property_data, pd.DataFrame)


def test_can_create_schema_for_sqlite(property_data):
    schema = create_schema(property_data)
    split = schema.split(",")
    assert len(split) == len(property_data.columns)
    for split_var, col_var in zip(split, property_data.columns):
        assert split_var == col_var

def test_create_table_takes_a_connection_argument(property_data, connection):
    con = connection
    schema = create_schema(property_data)
    table = create_table(con, table_name="property_sales_stg_sample", schema=schema)
    assert table is not None
    

def test_have_create_table_command(property_data, connection):
    con = connection
    schema = create_schema(property_data)
    res = create_table(con, table_name="property_sales_stg_sample", schema=schema)
    assert res is not None

def test_can_create_connection_object():
    """it's generally a good idea to create a connection once and re-use it"""
    db_name = "property.db"
    con = create_connection(db_name)
    assert isinstance(con, spatialite.connection.Connection)

def test_can_load_data_into_table(property_data, connection):
    con = connection
    schema = create_schema(property_data)
    table_name = create_table(con, table_name="property_sales_stg_sample", schema=schema)
    load_result = load_data_into_table(con, table_name, data=property_data, if_exists="replace")
    assert load_result is not None


def test_can_load_geocoded_data(geocoded_data, connection):
    con = connection
    load_result = load_data_into_table(con, table_name="property_sales_geocoded_sample", data=geocoded_data, if_exists="replace")
    assert load_result is not None


def test_can_load_shapefile(shapefile_path):
    load_result = load_shapefile(shapefile_path=shapefile_path,
                                 table_name="electoral_districts_2011")


def test_can_get_data_from_db(connection):
    table = 'property_sales_stg'
    n = 10
    query = f"select * from {table} limit {n}"
    result = get_data_from_db(connection, query)
    assert result.shape[0] == n

    
def test_can_load_data_from_ungeocoded_table(connection):
    table_name = "property_sales_stg_sample"
    num_results = 10
    result = get_property_data(connection=connection,
                               table_name = table_name,
                               num_results = num_results)
    assert result.shape[0] == num_results
    
