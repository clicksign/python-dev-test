import json
import requests


def counter_payload(item):
    """
        Prepares paylod to send to database.

        params
        ------------
            df:   Dataframe
    """
    payload = {}

    payload['counter'] = item.get('counter')

    return payload


def send_counter(df):
    """
        Makes the request to the database endpoint.

        params
        ------------
            df:   Dataframe
    """

    url = "http://127.0.0.1:8001/api/v1/census-etl/counter"
    headers = {
        "Content-Type": "application/json",
    }

    for item in df:
        payload = counter_payload(item)
        response = requests.request("POST", url, json=payload, headers=headers)
        # for load in payload:
        #     # print(f'json_payload: {item}')

    try:
        return json.loads(response.text)
    except Exception as e:
        print('Ingestion error: ', e)
        return "error"
