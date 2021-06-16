import os
import datetime
import matplotlib.pyplot as pyplot
import numpy
from django.template.loader import get_template
from django.template import Context
import pdfkit


def _create_two_grouped_bar_graph_in(title: str,
                                     y_title: str,
                                     labels: list,
                                     bar_values_1: list,
                                     bar_values_2: list,
                                     bar_label_1: str,
                                     bar_label_2: str,
                                     analysis_folder_path: str):
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
    axis.bar_label(bars1, padding=3)
    axis.bar_label(bars2, padding=3)
    figure.tight_layout()
    analysis_folder_graph_pdf_path = os.path.join(analysis_folder_path, "graphs", f"{title}.png")
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
