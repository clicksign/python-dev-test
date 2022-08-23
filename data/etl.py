import os

from functions import *
from send_data import send_data


def handle_file():

    FILE_PATH = '/home/bereoff/repos/python-dev-test/data/Adult.data'
    DESCRIPTION_PATH = '/home/bereoff/repos/python-dev-test/data/Description'

    steps = 5
    text = read_description_file(description_file=DESCRIPTION_PATH)
    names = generate_column_name(text=text)
    dtypes = generate_column_type(text=text)
    data = get_data(
        file_path=FILE_PATH,
        steps=steps,
        names=names,
        dtypes=dtypes
    )

    checkpoint_batch(df=data, steps=steps)
    send_data(df=data)


def main():
    handle_file()


if __name__ == '__main__':
    main()
