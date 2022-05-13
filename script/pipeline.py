import sys, pickle, os

from extract import load_adult_datasets

num_lines_to_process = 1630

# Optionals for running the pipeline
# run_eda = False
# get_insights = True

def main (auto = False):
    # Extract (get datasets)
    adult_data, adult_test = load_adult_datasets(
        first_n_lines = None if not auto else num_lines_to_process,
        skip_lines = num_lines_to_process
    )
