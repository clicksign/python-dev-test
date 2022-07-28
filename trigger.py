from extract_transform import run_extract_transform
from second_method_load_base_segmentada import run_insert_db_final
import time

if __name__ == "__main__":
    run_extract_transform()

    while True:
        run_insert_db_final(slice_len=1630)
        time.sleep(10)
        print("Insercao de lote concluida")
        print("-------------------------")
