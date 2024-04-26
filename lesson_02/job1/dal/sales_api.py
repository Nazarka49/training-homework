from typing import List, Dict, Any
import requests
import os

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")

def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': date, 'page': 2},
        headers={'Authorization': AUTH_TOKEN},
    )
    print("Response status code:", response.status_code)
    return response.json()
