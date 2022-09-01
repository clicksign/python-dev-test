# Criamos o banco de dados
db = peewee.SqliteDatabase('codigo_avulso.db')


class BaseModel(peewee.Model):
    """Classe model base"""

    class Meta:
        # Indica em qual banco de dados a tabela
        # 'author' sera criada (obrigatorio). Neste caso,
        # utilizamos o banco 'codigo_avulso.db' criado anteriormente
        database = db


#class Adult(BaseModel):
    #TODO: pegar os fields da description e ajustar ao model
