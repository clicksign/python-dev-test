import sys, pickle, os
from script.extract import load_adult_datasets
from script.load import insert_data
from script.transform import handle_missing_data
from script.load import insert_data
from script.eda import run_exploratory_data_analysis

num_lines_to_process = 1630

# Optionals for running the pipeline
run_eda = True
# get_insights = True

def pipeline_main (auto = False):
    chunk_read = 0
    if os.path.exists('chunk_read.pkl'):
        chunk_read = pickle.load(open('chunk_read.pkl', 'rb'))

        print("Chunks read: ", chunk_read)

    # ETL Phase 1 - Extract >> Get datasets 
    adult_data, adult_test = load_adult_datasets(
        first_n_lines = None if not auto else num_lines_to_process,
        skip_lines = chunk_read * num_lines_to_process
    )

    # ELT Phase 2 - Transform >> Normalize data types and handle nulls
    for data in [(adult_data, "AdultData"), (adult_test, "AdultTest")]: # Name used to create the output file of EDA
        dataset, name = data

        # Handle missing data
        dataset = handle_missing_data(dataset)

        # >> Optional << Run exploratory data analysis
        if run_eda and not auto:
            run_exploratory_data_analysis(dataset, name)

    # ETL Phase 3 - Load >> Load datasets into database
    insert_data("adult_data", adult_data)

if __name__ == '__main__':
    # Check if script is being executed by crontab
    pipeline_main('periodic' in sys.argv)
