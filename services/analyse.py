import os
import datetime
import sqlite3

import matplotlib.pyplot as pyplot
import numpy
import pandas
from django.template.loader import get_template
from django.template import Context
import pdfkit
from .variables import VARIABLES
from .sqlite import sqlite_table_exists, sqlite_get_dataframe_from


def _two_grouped_bar_creator(analysis_relation: list, dataframe: pandas.DataFrame, analysis_folder_path: str):
    """
    Organizes the content to create a two grouped bar graph
    @type analysis_relation: list
    @type dataframe: pandas.Dataframe
    @type analysis_folder_path: str
    @param analysis_relation: a list representing the information to convert
    @param dataframe: a dataframe representing the database
    @param analysis_folder_path: a string representing the path to graph image
    """
    expected_values_and_types = VARIABLES["expected_values_and_types"]
    expected_header = VARIABLES["expected_header"]
    column_1 = analysis_relation[0][0]
    value_1 = analysis_relation[0][1]
    column_2 = analysis_relation[1][0]
    value_2 = analysis_relation[1][1]
    consideration_column = analysis_relation[2]
    consideration_column_index = expected_header.index(consideration_column)
    consideration_column_expected_values = expected_values_and_types[consideration_column_index]
    title = f"{value_1} and {value_2} per {consideration_column}"
    y_title = "Count"
    temp_labels = consideration_column_expected_values
    labels = []
    bar_values_1 = []
    bar_values_2 = []
    temp_bar_values_1 = {}
    temp_bar_values_2 = {}
    column_1_relations = dataframe.value_counts([column_1, consideration_column]).to_dict()
    column_2_relations = dataframe.value_counts([column_2, consideration_column]).to_dict()
    for column_1_relation in column_1_relations:
        if column_1_relation[0] == value_1:
            temp_bar_values_1[column_1_relation[1]] = column_1_relations[column_1_relation]
    for column_2_relation in column_2_relations:
        if column_2_relation[0] == value_2:
            temp_bar_values_2[column_2_relation[1]] = column_2_relations[column_2_relation]
    for temp_label in temp_labels:
        if temp_label in temp_bar_values_1 and temp_label in temp_bar_values_2:
            bar_values_1.append(temp_bar_values_1[temp_label])
            bar_values_2.append(temp_bar_values_2[temp_label])
            labels.append(temp_label)
    _create_two_grouped_bar_graph_in(title,
                                     y_title,
                                     labels,
                                     bar_values_1,
                                     bar_values_2,
                                     value_1,
                                     value_2,
                                     analysis_folder_path)


def _create_two_grouped_bar_graph_in(title: str,
                                     y_title: str,
                                     labels: list,
                                     bar_values_1: list,
                                     bar_values_2: list,
                                     bar_label_1: str,
                                     bar_label_2: str,
                                     analysis_folder_path: str):
    """
    Creates a two grouped bar PNG in {analysis_folder_path}
    @type title: str
    @type y_title: str
    @type labels: list
    @type bar_values_1: list
    @type bar_values_2: list
    @type bar_label_1: list
    @type bar_label_2: str
    @type analysis_folder_path: str
    @param title: a string representing the graph title
    @param y_title: a string representing the graph y axis title
    @param labels: a list of strings representing its labels
    @param bar_values_1: a list of integers representing the first bar values
    @param bar_values_2: a list of integers representing the second bar values
    @param bar_label_1: a string representing the first bar values title
    @param bar_label_2: a string representing the second bar values title
    @param analysis_folder_path: a string representing the path to graph image
    """
    x = numpy.arange(len(labels))
    width = 0.35
    figure, axis = pyplot.subplots()
    bars1 = axis.bar(x - width / 2, bar_values_1, width, label=bar_label_1)
    bars2 = axis.bar(x + width / 2, bar_values_2, width, label=bar_label_2)
    axis.set_ylabel(y_title)
    axis.set_title(title)
    axis.set_xticks(x)
    axis.set_xticklabels(labels)
    axis.legend()
    axis.bar_label(bars1, padding=2)
    axis.bar_label(bars2, padding=2)
    figure.tight_layout()
    figure.autofmt_xdate()
    analysis_folder_graph_pdf_path = os.path.join(analysis_folder_path, "graphs", f"{title}.png")
    if VARIABLES["verbosity"]:
        print(f"Creating {analysis_folder_graph_pdf_path}!")
    pyplot.savefig(analysis_folder_graph_pdf_path)


def create_pdf_from_to(context: Context, analysis_pdf_folder_path: str):
    """
    Renders data and template.html based on {context} and creates it in {analysis_pdf_folder_path}
    @type context: str
    @type analysis_pdf_folder_path: Context
    @param context: a context representing the data to be rendered with template.html
    @param analysis_pdf_folder_path: a string representing the path to rendered template.html
    """
    template = get_template("template.html")
    rendered_template = template.render(context)
    pdfkit.from_string(rendered_template, analysis_pdf_folder_path)


def graph_dispatcher(analysis_folder_path: str) -> bool:
    """
    Reads analysis_relation and perform the graphs dispatching,
    then creates graphs related to its type and variables
    @type analysis_folder_path: str
    @param analysis_folder_path: a string representing the path to graph image
    @return: a boolean representing the dispatching success
    """
    analysis_relation = VARIABLES["analysis_relation"]
    with sqlite3.connect("SQLite_ClickSign.db") as connection:
        if sqlite_table_exists(connection, "data"):
            dataframe = sqlite_get_dataframe_from(connection, "data")
        else:
            return False
    for relation in analysis_relation:
        if relation[0] == "two_grouped_bar":
            two_grouped_bar_analysis_relation = relation[1]
            _two_grouped_bar_creator(two_grouped_bar_analysis_relation, dataframe, analysis_folder_path)
    return True


def create_analysis_folder() -> str:
    """
    Verifies if "data/analysis" exists. If not, create it. Then creates
    a analysis folder in "data/analysis" with datetime.now() as its name.
    @rtype: str
    @return: a string representing the created analysis folder path
    """
    analysis_path = os.path.join(os.getcwd(), "data", "analysis")
    analysis_path_exists = os.path.exists(analysis_path)
    if not analysis_path_exists:
        os.mkdir(analysis_path)
    now = datetime.datetime.now()
    analysis_folder_name = str(now)[:19]
    for element in [" ", "-", ":"]:
        analysis_folder_name = analysis_folder_name.replace(element, "")
    analysis_folder_path = os.path.join(analysis_path, analysis_folder_name)
    os.mkdir(analysis_folder_path)
    analysis_folder_graph_path = os.path.join(analysis_folder_path, "graphs")
    os.mkdir(analysis_folder_graph_path)
    return analysis_folder_path
