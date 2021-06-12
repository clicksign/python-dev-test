from tests import StandaloneTests

def validation(dataframe):
    pass

def main():
    output_file_exists = StandaloneTests.test_output_file_existence()
    validation()


if __name__ == '__main__':
    main()

