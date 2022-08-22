import json
import requests


def prepare_payload(df):
    """
        Prepares paylod to send to database.

        params
        ------------
            df:   Dataframe
    """

    payload = []
    for index, df in df.iterrows():
        if df['is_correct'] == False:
            continue
        else:
            payload.append(
                {
                    "age": int(df['age']),
                    "workclass": df['workclass'],
                    "fnlwgt": int(df["fnlwgt"]),
                    "education": df["education"],
                    "education_num": int(df["education_num"]),
                    "marital_status": df["marital_status"],
                    "occupation": df["occupation"],
                    "relationship": df["relationship"],
                    "race": df["race"],
                    "sex": df["sex"].lower(),
                    "capital_gain": int(df["capital_gain"]),
                    "capital_loss": int(df["capital_loss"]),
                    "hours_per_week": int(df["hours_per_week"]),
                    "native_country": df["native_country"],
                    "class_category": df["class_category"]
                }
            )
    json_payload = payload
    for load in payload:
        print(f'json_payload: {load}')
    return json_payload


def send_data(df):
    """
        Makes the request to the database endpoint.

        params
        ------------
            df:   Dataframe
    """

    payload = prepare_payload(df)
    url = "http://localhost:8000/api/v1/census-etl/"
    headers = {
        "Content-Type": "application/json",
    }
    response = requests.request("POST", url, json=payload, headers=headers)
    try:
        return json.loads(response.text)
    except Exception as e:
        print('Ingestion error: ', e)
        return "error"
