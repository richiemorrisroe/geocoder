import pytest

from geocode.osm import get_geocode_from_address, convert_response_to_json

def test_get_geocode_from_address_returns_something():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    assert result is not None

def test_get_geocode_from_address_returns_some_content():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    assert result.content is not None


def test_convert_response_to_json_returns_json():
    address = "17 castleknock brook, castleknock, dublin, ireland"
    result = get_geocode_from_address(address)
    result_json = convert_response_to_json(result)
    assert result_json is not None        

