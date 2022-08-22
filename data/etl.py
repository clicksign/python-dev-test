import os

from functions import *
from send_data import send_data


def handle_file():

    description_file = os.getcwd() + '/data/Description'
    file_path = os.getcwd() + '/data/Adult.data'
    steps = 10

    text = read_description_file(description_file=description_file)
    names = generate_column_name(text=text)
    dtypes = generate_column_type(text=text)

    data = get_data(
        file_path=file_path,
        steps=steps,
        names=names,
        dtypes=dtypes
    )

    get_removed_data(df=data)

    checkpoint_batch(df=data)

    send_data(df=data)


def main():
    handle_file()


if __name__ == '__main__':
    main()
