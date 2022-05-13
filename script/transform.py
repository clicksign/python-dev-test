def handle_missing_data(data):
    """
    Check for missing data in dataset and
    replaces values with mean or most common ones
    Return: Data transformed
    """

    # Get summary of nulls in the dataset
    missing_data = dict(data.isnull().sum())

    for column, missing_count in missing_data.items():
        if missing_count > 0:
            print(f"{column} has {missing_count} missing values.")

            # Fill missing data with column mean if numeric,
            # otherwise, with the most common value
            if data[column].dtype == 'int64' or data[column].dtype == 'float64':
                print(f"{column} has missing values. Replacing with mean.\n")
                data[column].fillna(data[column].mean(), inplace=True)
            else:
                print(f"{column} has missing values. Replacing with mode.\n")
                data[column].fillna(data[column].mode()[0], inplace=True)

    return data