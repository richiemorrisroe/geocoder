import json
from geocode.geocode_funcs import create_logger
from geocode.join import add_ireland_to_address
from geocode.sql import check_for_new_rows, create_connection
import pytest

from geocode.osm import geocode_addresses, get_geocode_from_address, convert_response_to_json

@pytest.fixture
def connection():
    return create_connection('property.db')

@pytest.mark.skip
def test_get_geocode_from_address_returns_something():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    assert result is not None

@pytest.mark.skip    
def test_get_geocode_from_address_returns_some_content():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    print(f"{result.url=}")
    assert result.content is not None


@pytest.mark.skip    
def test_convert_response_to_json_returns_json():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    result_json = convert_response_to_json(result)
    print(f"{result_json=}")
    assert isinstance(result_json, dict)


@pytest.mark.skip    
def test_convert_response_to_json_returns_lat_and_lon():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    result_json = convert_response_to_json(result)
    print(f"{result_json.keys()=}")
    assert result_json['lat'] is not None and result_json['lon'] is not None

@pytest.mark.skip
def test_can_geocode_random_addesses(connection):
    limit = 100
    logger = create_logger()
    gc_adds = geocode_addresses(connection, "address", limit=limit, logger=logger)
    sum_correct = gc_adds.data.notnull().sum()
    print(f"{gc_adds.data.head()=}")
    print(f"{sum_correct=}")    # 
    assert len(gc_adds) == limit-1
