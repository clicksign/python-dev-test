from src.config.database import DB
from sqlalchemy import orm

class BaseRepository():

    '''
    Classe criada para ser herdada pelos repositories, isso facilita a criacao das sessions e
    gerenciamento de scopo da ORM
    '''

    def __init__(self, db: DB):
        
        self._db = db
        self.entity = None
        self._session_factory = orm.scoped_session(
            orm.sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._db.get_engine(),
            ),
        )
        self.session: orm.Session = self._session_factory()
        
    @property
    def entity(self):
        return self._entity
    
    @entity.setter
    def entity(self, entity):
        self._entity = entity

    def save(self, bulk=False):
        with self.session as session:
            if bulk:
                session.bulk_save_objects(self.entity)
            else:
                session.add(self.entity)
            session.flush()
            session.expunge_all()
            session.commit()
