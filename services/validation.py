import pandas
from .initial_clean import initial_clean
from .standalone_tests import StandaloneTests
from .variables import VARIABLES


def validate(dataframe: pandas.DataFrame):
    dataframe = dataframe.astype(str)
    expected_values_and_types = VARIABLES["expected_values_and_types"]
    expected_header = VARIABLES["expected_header"]
    unwelcome_chars_and_words = VARIABLES["unwelcome_chars_and_words"]
    output_file_path = VARIABLES["output_file_path"]
    for index, row in dataframe.iterrows():
        for column_number in expected_values_and_types:
            column_name = expected_header[column_number]
            string = row.to_dict()[column_name]
            is_type = type(expected_values_and_types[column_number]) is type
            if is_type:
                is_an_expected_type = StandaloneTests.test_non_expected_types(string, column_number)
                if not is_an_expected_type:
                    dataframe.drop(index, inplace=True)
                    break
            else:
                is_an_expected_value = StandaloneTests.test_non_expected_values(string, column_number)
                if not is_an_expected_value:
                    dataframe.drop(index, inplace=True)
                    break
            for unwelcome_char_and_word in unwelcome_chars_and_words:
                welcome_chars_and_words = unwelcome_chars_and_words[unwelcome_char_and_word]
                string = string.replace(unwelcome_char_and_word, welcome_chars_and_words)
            dataframe.loc[index, column_name] = string.strip()
    dataframe.to_csv(output_file_path, index=False)


def main():
    output_file_path = VARIABLES["output_file_path"]
    output_file_exists = StandaloneTests.test_output_file_existence(output_file_path)
    if not output_file_exists:
        initial_clean()
    output_file_dataframe = pandas.read_csv(output_file_path)
    validate(output_file_dataframe)


if __name__ == '__main__':
    main()
