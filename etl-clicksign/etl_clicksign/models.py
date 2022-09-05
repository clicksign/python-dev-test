import sys
sys.path.append('../')

import peewee
from peewee import Check
from etl_clicksign.enumerations import WORKCLASS, CLASSES

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
    education = peewee.CharField()
    education_num = peewee.IntegerField()
    marital_status = peewee.CharField()
    occupation = peewee.CharField()
    relationship = peewee.CharField()
    race = peewee.CharField()
    sex = peewee.CharField()
    capital_gain = peewee.IntegerField()
    capital_loss = peewee.IntegerField()
    hours_per_week = peewee.IntegerField()
    native_country = peewee.CharField()
    _class = peewee.CharField(choices=CLASSES)

    class Meta:
        constraints = [
            Check('age>-1'),
            Check('fnlwgt>-1'),
            Check('education_num>-1'),
            Check('capital_gain>-1'),
            Check('capital_loss>-1'),
            Check('hours_per_week>-1')
        ]
