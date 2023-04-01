import json

import requests
import urllib.parse
BASE_URL = "http://localhost:8080"

def get_geocode_from_address(address, base_url=BASE_URL):
    query = urllib.parse.quote(address)
    query_url = f"{base_url}/search/q={query}"
    result = requests.get(query_url)
    return result

def convert_response_to_json(result):
    if isinstance(result, requests.Response):
        content = result.content
    result = json.loads(content)
    return result
