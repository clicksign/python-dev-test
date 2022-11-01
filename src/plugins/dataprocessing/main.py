from utils.get_data import get_data
import pandas as pd
import re
import logging

logging.getLogger("Data Processing")

def main(path_file, cols_type):
    re_string_values = re.compile(r"[\?\!\#\@\$]")
    re_num_values = re.compile(r'[0-9\.]+')

    logging.info("Load Data")
    try:
        data = get_data(path_file)
        df = pd.DataFrame(data = data, columns = cols_type.keys())
    except Exception as e:
        logging.error(f"Failure to load Data with path {path_file}")
        
    df = df.drop_duplicates()
    cols_num = [x for x in cols_type.keys() if cols_type[x] in ("int", "float")]
    cols_str = [x for x in cols_type.keys() if cols_type[x] in ("varchar", "object", "category")]

    logging.info("Fit the numeric types of the columns")
    for col in cols_num:
        if cols_type[col] in ("float", "int"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logging.info("Fit the inconsistents string types of the columns")
    def fixin_data_str(x, cols):
        for col in cols:
            if re.fullmatch(re_string_values, str(x[col]).strip()):
                x[col] = None
        return x

    df = df.apply(fixin_data_str, axis=1, args =(cols_str,))
    df['age'] = df["age"].fillna(0)
    df["fnlwgt"] = df["fnlwgt"].fillna(0)
    df.dropna(subset=['native-country'], inplace = True)
    
    logging.info("Data Processing finish")
    return df