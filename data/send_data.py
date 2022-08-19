import json
import requests


def prepare_payload(item):
    payload = [
        {
            "age": item.get("age", None),
            "workclass": item.get("workclass", None),
            "fnlwgt": item.get("fnlwgt", None),
            "education": item.get("education", None),
            "education-num": item.get("education-num", None),
            "marital-status": item.get("marital-status", None),
            "occupation": item.get("occupation", None),
            "relationship": item.get("relationship", None),
            "race": item.get("race", None),
            "sex": item.get("sex", None),
            "capital_gain": item.get("capital_gain", None),
            "capital_loss": item.get("capital_loss", None),
            "hours_per_week": item.get("hours_per_week", None),
            "native_country": item.get("native_country", None),
            "class": item.get("class", None)
        }
    ]

    return payload


def create_register(payload):
    data = json.dumps(payload)
    url = "http://127.0.0.1:8000/api/v1/census-etl"
    response = requests.request("POST", url, json=payload)
    print(response.text)
    return response


def send_data(df):
    for item in df:
        payload = prepare_payload(item)
        data = json.dumps(payload)
        response = create_register(payload)

    return print(response)
