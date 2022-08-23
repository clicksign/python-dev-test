from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group

import os
import time

from data.functions import *
from data.send_data import send_data


class Command(BaseCommand):

    def handle(self, **options):

        FILE_PATH = '/home/bereoff/repos/python-dev-test/data/Adult.data'
        DESCRIPTION_PATH = '/home/bereoff/repos/python-dev-test/data/Description'
        steps = 1630
        text = read_description_file(description_file=DESCRIPTION_PATH)
        names = generate_column_name(text=text)
        dtypes = generate_column_type(text=text)

        for i in range(19):

            data = get_data(
                file_path=FILE_PATH,
                steps=steps,
                names=names,
                dtypes=dtypes
            )

            checkpoint_batch(df=data, steps=steps)
            send_data(df=data)
            time.sleep(10)
