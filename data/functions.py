import pandas as pd


def normalizer(line):
    '''
    normalize the text to column names
    '''
    return line.replace(':', '').strip()


def get_data(path, steps, counter, names):
    """
    Returns a subset of rows from a file. The fist [steps]*[count] 
    rows are skipped and the next [steps] rows are returned. 

    params
    ------------
        steps:   number of rows returned
        counter: count variable updated each iteration 
        names:   columns names of dataset
        path:    location of csv
    """

    if counter == 0:
        df = pd.read_csv(path, nrows=steps, names=names)
    else:
        df = pd.read_csv(path, skiprows=steps*counter,
                         nrows=steps, names=names)
    return df


def get_counter():
    '''
    Get number of counts to consider the
    next batch
    '''
    if os.path.exists('checkpoint.json') is False:
        counter = 0
    else:
        data = pd.read_json('checkpoint.json')
        counter = data['counter'].max()

    return counter


def checkpoint_batch(df):
    '''
    Verifies data checkpoint
    '''
    for index, row in df.iterrows():

        # Update count
        counter += 1
        df['counter'] = counter
        df['last_index'] = counter*steps

        if len(df) != steps:
            break
        else:
            df.to_json(os.getcwd()+'/checkpoint.json')
            break

        # Exit loop

    print(f'Number of ingestion: {counter}')
    print(f'Total of ingestion: {counter*steps}')
