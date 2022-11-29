from src.infrastructure.core import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils.functions import database_exists, create_database


class DbConnection:
    """
    This class creates an engine and a database session
    """

    def __init__(self, url: str) -> None:
        self._url = url

    def engine(self):
        if not database_exists(self._url):
            create_database(self._url)
        return create_engine(self._url)

    def session(self):
        return scoped_session(sessionmaker(
            bind=self.engine(),
            autocommit=False,
            autoflush=False,
        ))


dbconn = DbConnection(Settings.SQLALCHEMY_DATABASE_URI.value)  # noqa
