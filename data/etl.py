import pandas as pd
import os
import re

from .functions import *
from .send_data import send_data


def handle_file():
    with open("Description", 'r') as f:
        text = f.read().splitlines()

    columns = []
    for sentences in text:
        lines = map(normalizer, re.findall('^\s*\D[^:]+:\s*', sentences))
        for line in lines:
            if line not in columns:
                columns.append(line.replace('-', '_'))
            else:
                continue
    length = len(columns)
    columns = columns[:length-2]

    PATH = os.getcwd() + '/Adult.data'
    STEPS = 1630
    NAMES = columns

    length = len(columns)

    columns = columns[:length-2]

    counter = get_counter()

    data = get_data(
        path=PATH,
        steps=STEPS,
        counter=counter,
        names=NAMES
    )

    data_batch(data)

    send_data(data.to_dict(orient='records'))


def main():
    handle_file()


if __name__ == '__main__':
    main()
