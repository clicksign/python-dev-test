import sweetviz as sv
import os

def run_exploratory_data_analysis(data, name):
    """
    Runs a exploratory data analysis on the dataset.
    """

    #create folder if not exists
    if not os.path.exists('EDA'):
        os.makedirs('EDA')

    data_report = sv.analyze(data, pairwise_analysis="on")
    data_report.show_html(filepath=f"EDA/{name}.html", open_browser=False)