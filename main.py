import pandas
from tests.variables import VARIABLES

data_file_path = VARIABLES["data_file_path"][3:]

adult_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )

adult_file_dataframe.info()


def main():
    pass


if __name__ == '__main__':
    main()
    