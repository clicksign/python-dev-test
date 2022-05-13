import pandas as pd
from pandarallel import pandarallel

# Using pandarallel to speed up pandas .apply() by using multiple cores
pandarallel.initialize()

def parse_header_info():
    """
    Parses the header information from the description file.
    Return: Two lists, one for the headers and one for the data types
    """
    headers = []
    data_type = []
    lines = ""

    # Read Description File
    with open('../data/Description', 'r') as f:

        # Read entire content
        lines = f.read()

    # Replace line breaks after colon, so all columns information is on one line
    lines = lines.replace(': \n', ':').split('\n')

    attributes_index_start = 0

    # Find line that contains "Attribute type"
    for i in range(len(lines)):
        if "Attribute type" in lines[i]:

            # If the line has "Attribute type" in it, then than the attributes start after that line
            attributes_index_start = i + 1

    # Iterate until all attributes are found and stopping when line is empty
    for i in range(attributes_index_start, len(lines)):
        if lines[i].strip() == '':
            break

        header_metadata = lines[i].split(':')
        headers.append(header_metadata[0].strip().replace('-', '_')) # Better _ than - to DB

        is_data_type_numeric = 'continuous' in header_metadata[1]
        data_type.append(is_data_type_numeric)

    return headers, data_type



def try_convert_numeric(value):
    """
    Tries to convert a value to a numeric value
    Return: Numeric value or original value
    """
    try:
        return float(value)
    except Exception:
        return None
    

def load_adult_datasets(first_n_lines = None, skip_lines = 0):
    """
    Loads the adult datasets
    Return: Dataset files
    """

    headers, data_type = parse_header_info()

    # Read the data and inject parsed headers
    adult_data = pd.read_csv('../data/Adult.data', header=None, sep=',', skiprows=skip_lines, nrows=first_n_lines)

    # Read the test data and inject parsed headers skipping first line
    adult_test = pd.read_csv('../data/Adult.test', header=None, sep=',', skiprows=1 + skip_lines, nrows=first_n_lines)

    # Iterate both datasets because they have the same information 
    # so avoid code duplication
    for data in [adult_data, adult_test]:

        # Inject headers and covert data types if needed
        data.columns = headers

        for i in range(len(headers)):
            if data_type[i]:
                data[headers[i]] = data[headers[i]].parallel_apply(lambda x: try_convert_numeric(x))
            else:
                data[headers[i]] = data[headers[i]].astype(str)
                data[headers[i]] = data[headers[i]].parallel_apply(lambda x: None if x.strip() == '?' else x.strip())

    return adult_data, adult_test
                    

