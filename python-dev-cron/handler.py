import datetime
import logging

from functions import *
from send_data import *
from send_counter import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    current_time = datetime.datetime.now().time()
    name = context.function_name
    logger.info("Your cron function " + name + " ran at " + str(current_time))

    FILE_PATH = '/home/bereoff/repos/python-dev-test/data/Adult.data'
    DESCRIPTION_PATH = '/home/bereoff/repos/python-dev-test/data/Description'

    steps = 5
    text = read_description_file(description_file=DESCRIPTION_PATH)
    names = generate_column_name(text=text)
    dtypes = generate_column_type(text=text)
    data = get_data(
        file_path=FILE_PATH,
        steps=steps,
        names=names,
        dtypes=dtypes
    )

    checkpoint_batch(df=data, steps=steps)
    send_data(df=data)
