import sys
sys.path.append('../')

from random import choices
import peewee
from etl_clicksign.enumerations import WORKCLASS

# Criamos o banco de dados
# Aqui pode ser um PostgreSql porem da mais complexidade de configuracao do setup do projeto
db = peewee.SqliteDatabase('Adult.db')

class BaseModel(peewee.Model):
    """Classe model base"""

    class Meta:
        # Indica em qual banco de dados a tabela
        # utilizamos o banco 'Adult.db' criado anteriormente
        database = db


class Adult(BaseModel):
    age = peewee.IntegerField()
    workclass = peewee.CharField(choices=WORKCLASS)
    fnlwgt = peewee.IntegerField()
    #TODO: pegar os fields da description e ajustar ao model
