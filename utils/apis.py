import requests


def get_data_api_json(url):
    result = requests.get(url).json()
    return result
