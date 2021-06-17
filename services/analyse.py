import os
import datetime
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
from django.template.loader import get_template
from django.template import Context
import pdfkit
from .variables import VARIABLES
from .sqlite import sqlite_table_exists, sqlite_get_dataframe_from


def _two_grouped_bar_creator(analysis_relation: list, dataframe: pd.DataFrame, analysis_folder_path: str):
    """
    Creates a two grouped bar graph based in {analysis_relation}
    @type analysis_relation: list
    @type dataframe: pd.Dataframe
    @type analysis_folder_path: str
    @param analysis_relation: a list representing the information to convert
    @param dataframe: a dataframe representing the database
    @param analysis_folder_path: a string representing the path to graph image
    """
    index = []
    value_1 = analysis_relation[0][1]
    value_2 = analysis_relation[1][1]
    plot_dict = {value_1: [], value_2: []}
    column_1 = analysis_relation[0][0]
    column_2 = analysis_relation[1][0]
    consideration_column = analysis_relation[2]
    title = f"{value_1} and {value_2} per {consideration_column}"
    column_1_value_1_relations = dataframe.value_counts([column_1, consideration_column])[value_1]
    column_2_value_2_relations = dataframe.value_counts([column_2, consideration_column])[value_2]
    column_1_relations_dict = column_1_value_1_relations.to_dict()
    column_2_relations_dict = column_2_value_2_relations.to_dict()
    column_1_relations_index = sorted(column_1_value_1_relations.index)
    column_2_relations_index = sorted(column_2_value_2_relations.index)
    for index_1 in column_1_relations_index:
        index.append(index_1)
        plot_dict[value_1].append(column_1_relations_dict[index_1])
        if index_1 in column_2_relations_index:
            plot_dict[value_2].append(column_2_relations_dict[index_1])
        else:
            plot_dict[value_2].append(0)
    for index_2 in column_2_relations_index:
        if index_2 not in column_1_relations_index:
            index.append(index_2)
            plot_dict[value_1].append(0)
            plot_dict[value_2].append(column_2_relations_dict[index_2])
    plot_data = pd.DataFrame(plot_dict, index=index)
    plot_data.plot(kind="bar")
    plt.title(title)
    plt.xlabel(consideration_column)
    plt.ylabel("Count")
    analysis_folder_graph_pdf_path = os.path.join(analysis_folder_path, "graphs", f"{title}.png")
    plt.tight_layout()
    plt.savefig(analysis_folder_graph_pdf_path)


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
