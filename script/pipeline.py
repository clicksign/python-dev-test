import sys, pickle, os
from script.control import get_processed_lines_read, set_processed_lines_read, num_lines_to_process
from script.extract import load_adult_datasets
from script.insights import get_importance_of_attributes_to_class
from script.load import insert_data
from script.transform import handle_missing_data
from script.load import insert_data
from script.eda import run_exploratory_data_analysis
import time

# Optionals for running the pipeline
run_eda = False # Run exploratory data analysis
get_insights = True # Run Machine Learning Model

def pipeline_main (auto = False):
    # Get last processed lines from csv source files that was wrote inside pickle file
    if os.path.exists('processed_lines.pkl'):

        # Read pickle file
        processed_lines = pickle.load(open('processed_lines.pkl', 'rb'))
        # Set last processed lines from pickle file and add to global variables
        set_processed_lines_read(processed_lines['data'], processed_lines['test'])

    # Get last processed lines from global variables to be used in the next iteration
    lines_processed_data, lines_processed_test = get_processed_lines_read()

    # ETL Phase 1 - Extract >> Get datasets 
    adult_data, adult_test = load_adult_datasets(
        first_n_lines = None if not auto else num_lines_to_process,
        skip_lines = (lines_processed_data, lines_processed_test)
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
        insert_data(name, dataset)

    # >> Optional << Get insights
    if get_insights and not auto:
        get_importance_of_attributes_to_class(adult_data, 'class')

    # Get last processed lines from global variables to save into pickle file
    lines_processed_data, lines_processed_test = get_processed_lines_read()
    pickle.dump({ 'data': lines_processed_data, 'test': lines_processed_test }, open('processed_lines.pkl', 'wb'))

if __name__ == '__main__':
    periodic = 'periodic' in sys.argv

    # Check if script is being executed by crontab
    if periodic:
        # Measure time taken to run the script
        start_time = time.time()
        pipeline_main(periodic) # Run the pipeline for periodic execution
        
        # Crontab cannot be used to schedule a job in seconds interval.
        # So, we need to wait for a while to run the script again
        # This is a hack to make the script run again in a period of time
        for i in range(5):
            # Wait for 10 seconds before running the script again, because 1 minute has 60 seconds
            while (time.time() - start_time) <= 10:                
                time.sleep(0.02)

            pipeline_main(periodic)

            # Update start time for next iteration
            start_time = time.time() 

    else:
        # Run the pipeline for manual execution
        pipeline_main(False)
