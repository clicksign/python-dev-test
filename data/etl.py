import os

from .functions import *
from .send_data import send_data


def handle_file():
    description_file = os.getcwd() + '/Description'
    path = os.getcwd() + '/Adult.data'
    steps = 1630

    text = read_file(description_file)
    names = generate_column_name(text)
    dtypes = generate_column_type(text)

    data = get_data(
        file_path=file_path,
        steps=steps,
        names=names,
        dtypes=dtypes
    )

    get_removed_data(data)

    checkpoint_batch(data)

    send_data(data.to_dict(orient='records'))


def main():
    handle_file()


if __name__ == '__main__':
    main()
