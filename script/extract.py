import pandas as pd
from pandarallel import pandarallel

from script.control import set_processed_lines_read, num_lines_to_process

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
    with open('data/Description', 'r') as f:

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
    

def load_adult_datasets(first_n_lines = None, skip_lines = (0, 0)):
    """
    Loads the adult datasets
    Return: Dataset files
    """

    headers, data_type = parse_header_info()

    # Read the data and inject parsed headers    
    try: # Handle case when all lines from the file was read

        # Read the data with lines interval, so we can skip read lines
        adult_data = pd.read_csv('data/Adult.data', header=None, sep=',', skiprows=skip_lines[0], nrows=first_n_lines)
    except pd.errors.EmptyDataError:
        adult_data = pd.DataFrame(columns=headers)

    # Read the test data and inject parsed headers skipping first line
    try:
        adult_test = pd.read_csv('data/Adult.test', header=None, sep=',', skiprows=1 + skip_lines[1], nrows=first_n_lines)
    except pd.errors.EmptyDataError:
        adult_test = pd.DataFrame(columns=headers)

    # Inject processed lines read into global variables    
    if first_n_lines:
        set_processed_lines_read(
            # If file length is less than the amount of lines to process, 
            # then set to processed lines global variables the sum between the amount of lines that was set in the 'adult_data'
            # (because the length of this dataframe is the interval between the last line that was read and the amount of lines to process (1630))
            # plus the amount of lines that was skipped.
            # Otherwise, set to processed lines global variables the sum between the amount of lines to process (1630),
            # plus the last line that was read.
            len(adult_data) + skip_lines[0] if len(adult_data) < first_n_lines else skip_lines[0] + num_lines_to_process,
            len(adult_test) + skip_lines[1] if len(adult_test) < first_n_lines else skip_lines[1] + num_lines_to_process,
        )

    # Iterate both datasets because they have the same information 
    # so avoid code duplication
    for data in [adult_data, adult_test]:

        # Inject headers and covert data types if needed
        data.columns = headers

        if len(data) > 0:
            for i in range(len(headers)):
                if data_type[i]:
                    data[headers[i]] = data[headers[i]].parallel_apply(lambda x: try_convert_numeric(x))
                else:
                    data[headers[i]] = data[headers[i]].astype(str)
                    data[headers[i]] = data[headers[i]].parallel_apply(lambda x: None if x.strip() == '?' else x.strip())

    return adult_data, adult_test
                    

