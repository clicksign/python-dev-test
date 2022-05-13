import sys, pickle, os
from extract import load_adult_datasets
from transform import handle_missing_data

num_lines_to_process = 1630

# Optionals for running the pipeline
# run_eda = False
# get_insights = True

def main (auto = False):
    # ETL Phase 1 - Extract >> Get datasets 
    adult_data, adult_test = load_adult_datasets(
        first_n_lines = None if not auto else num_lines_to_process,
        skip_lines = num_lines_to_process
    )

    # ELT Phase 2 - Transform >> Normalize data types and handle nulls
    for data in [(adult_data, "AdultData"), (adult_test, "AdultTest")]: # Name used to create the output file of EDA
        dataset, name = data

        # Handle missing data
        dataset = handle_missing_data(dataset)


