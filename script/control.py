num_lines_to_process = 1630

lines_processed_data = 0
lines_processed_test = 0

def get_processed_lines_read():
    """
    Return last processed lines from both csv files
    """
    return lines_processed_data, lines_processed_test

def set_processed_lines_read(crd, crt):
    """
    Set last processed lines from both csv files into variables
    """
    global lines_processed_data
    global lines_processed_test

    lines_processed_data, lines_processed_test = crd, crt