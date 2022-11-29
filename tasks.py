from celery import Celery
from celery.result import AsyncResult
from sqlalchemy.engine import create_engine

celery_2 = Celery('app', broker='redis://127.0.0.1/1', backend='redis://127.0.0.1/2')


@celery_2.task()
def sand_mail_dummy_fun():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/rat_data_base')
    connection = engine.connect()
    names = connection.execute('''SELECT owner FROM "Notes"''').fetchall()
    for name in names:
        name_str = str(name).replace(',', '').replace("'", '').replace('(', '').replace(')', '')
        print(f'mail for name {name_str} sent')
    return names


def get_status(task_id: str):
    res = AsyncResult(task_id).state
    return res

