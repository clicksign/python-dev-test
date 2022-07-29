from extract_transform import run_extract_transform
from second_method_load_base_segmentada import run_insert_db_final
import time
from environment_variables import SLICE_LEN, TIME_SLEEP

if __name__ == "__main__":
    run_extract_transform()

    while True:
        try:
            sucess=run_insert_db_final(slice_len=SLICE_LEN)
            time.sleep(TIME_SLEEP)
            print("Insercao de lote concluida")
            print("-------------------------")
        except Exception as ex:
            print('ERRO NO SISTEMA')
            raise ex
