import sys
sys.path.append('../')

from etl_clicksign.models import Adult
from etl_clicksign.__init__ import __version__

def test_version():
    assert __version__ == '0.1.0'

def test_create_table_peewee():
    Adult.create_table()
    rec1=Adult(age=20, workclass="Private", fnlwgt=2)
    rec1.save()


test_version()
test_create_table_peewee()
