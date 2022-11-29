import schedule
import os

from src.infrastructure.core import Settings
from src.infrastructure.database.session import dbconn
from src.etl.service import EtlService
from datetime import datetime


def etl_flow():
    start = datetime.now()
    etl = EtlService()
    data = etl.select_dataset()
    processed_data = etl.transforms_data(data)
    amount_data_into_db = etl.save_in_db(processed_data)
    end = datetime.now()
    print(f"{amount_data_into_db} records were inserted in the database")
    print(f"Insertion time: {(end - start).total_seconds()} s")
    print('Loading dataset in db START!!!\n')


schedule.every(10).seconds.do(etl_flow)


def stop_execution() -> bool:
    """This function checks if the execution loop should continue or should end 
    """
    engine = dbconn.engine()
    if engine.dialect.has_table(engine.connect(), 'adult'):
        if not os.path.exists(f'{Settings.PATH.value}/data/AdultBuffer.data'):
            return True
    return False


if __name__ == '__main__':
    print('\n *** Starting ETL process ...')
    flag = True
    while flag:
        schedule.run_pending()
        if stop_execution():
            flag = False
            print(' *** Finished ETL process!\n')
