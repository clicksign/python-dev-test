import asyncio
import datetime
import multiprocessing
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
import aiofiles as aiofiles
import aiosqlite
from rocketry import Rocketry

MAX_ROWS_TO_PROCESS = 1630
path_to_file: str

sched = Rocketry()


@dataclass(frozen=True)
class PersonRow:
    age: int
    workclass: str
    fnlwgt: int
    education: str
    education_num: int
    marital_status: str
    occupation: str
    relationship: str
    race: str
    sex: str
    capital_gain: int
    capital_loss: int
    hours_per_week: int
    native_country: str
    class_: str


def accumulator():
    total = 0
    value = None
    while True:
        # receive sent value
        # yield total
        value = yield total
        if value is None: break
        # aggregate values
        total += value


class Counter:
    def __init__(self):
        self.__acc = accumulator()
        self.__acc.send(None)
        self.__quantity = 0

    def insert_(self, value):
        self.__quantity = self.__acc.send(value)

    @property
    def quantity(self):
        return self.__quantity

    @quantity.setter
    def quantity(self, value):
        self.__quantity = value

    def __call__(self, *args, **kwargs):
        return self.quantity


global counter


def ckeck_row_formation(row: str) -> bool:
    return len(row.split(',')) == 15


async def read_file(file_path):
    async with aiofiles.open(file_path, mode='r', newline='\n') as f:
        async for row in f:
            # print(row)
            if not ckeck_row_formation(row):
                print('Pulou linha')
                # print(row)
                continue
            yield row


async def get_connection():
    connection = await aiosqlite.connect(str(Path.joinpath(Path.cwd(), 'database.sqlite3')), )
    await connection.execute(
        'CREATE TABLE IF NOT EXISTS adult_table (age INT, workclass INT, fnlwgt INT, education CHAR, '
        'education_num INT, marital_status CHAR, occupation CHAR, relationship CHAR, race CHAR, sex CHAR, '
        'capital_gain INT, capital_loss INT, hours_per_week INT, native_country CHAR, class CHAR);'
    )
    return connection


async def clean_queue(queue: asyncio.Queue):
    while not queue.empty():
        await queue.get()


async def producer(queue):
    global counter
    async for record in read_file(path_to_file):
        if counter() >= MAX_ROWS_TO_PROCESS:
            await clean_queue(queue)
            break
        await queue.put(record)
    await queue.put(None)


async def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
    tasks = [task for task in asyncio.all_tasks(loop) if not (task.done() or task.cancelled())]
    if not tasks:
        return
    async for task in tasks:
        await asyncio.sleep(0)
        task.cancel()


#

@sched.task('every 10 seconds', execution="async")
async def etl():
    print(f'Início: {datetime.datetime.now()}')
    global counter
    counter = Counter()
    connection = await get_connection()
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count(), ) as pool:
        queue = asyncio.Queue(maxsize=int(MAX_ROWS_TO_PROCESS / multiprocessing.cpu_count()))
        loop = asyncio.get_running_loop()
        try:
            await asyncio.gather(
                asyncio.create_task(producer(queue)),
                asyncio.create_task(consumer(loop, pool, queue, connection)),
                return_exceptions=True
            )
        except Exception as err:
            print(err)
        finally:
            await connection.close()
            await _cancel_all_tasks(loop)
    print(f'Fim: {datetime.datetime.now()}')
            # sys.exit(1)
    # print(f'fim -> {datetime.now()}')


def transform(rows_to_transform: list):
    transformed_batch = []
    for _row in rows_to_transform:
        _row = str(_row).replace('\n', '').replace(' ', '').replace('?', '')
        person = PersonRow(*_row.split(','))
        transformed_batch.append(person)
    return transformed_batch


async def load(list_rows, connection):
    global counter
    async with connection.cursor() as cursor:
        len_list_rows = len(list_rows)
        if len_list_rows > 1:
            print(f'TAMANHO DA list_rows: {len_list_rows}')
        insert_rows = await cursor.executemany(
            "INSERT INTO adult_table(age,workclass,fnlwgt,education,education_num,marital_status\
            ,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week\
            ,native_country,class) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
            , [list(row.__dict__.values()) for row in list_rows]
        )
        print(f'insert_rows: {insert_rows.rowcount}')
        await connection.commit()
        counter.insert_(insert_rows.rowcount)


async def load_rows(to_do_rows_set, connection):
    for list_rows in to_do_rows_set:
        await load(await list_rows, connection)


async def consumer(loop, pool, queue, connection):
    to_do_rows_set = set()
    batch = []
    global counter
    while True:
        row = await queue.get()
        if counter() >= MAX_ROWS_TO_PROCESS:
            await _cancel_all_tasks(loop)
        # se receber None indica final das linhas
        if row is not None:
            batch.append(row)
        if queue.empty():
            task = loop.run_in_executor(pool, transform, batch)
            to_do_rows_set.add(task)
            if len(to_do_rows_set) >= pool._max_workers:
                finished_rows_set, to_do_rows_set = await asyncio.wait(to_do_rows_set,
                                                                       return_when=asyncio.FIRST_COMPLETED)
                await load_rows(finished_rows_set, connection)
            batch = []
        if row is None:
            break
    if to_do_rows_set:
        await load_rows(asyncio.as_completed(to_do_rows_set), connection)


# async def main():
#     """Launch Rocketry app (and possibly something else)"""
#     rocketry_task = asyncio.create_task(sched.serve())
#     await rocketry_task


if __name__ == '__main__':
    path_to_file = sys.argv[1]
    MAX_ROWS_TO_PROCESS = int(sys.argv[2])
    sched.run()

