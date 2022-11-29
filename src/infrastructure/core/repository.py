from typing import Dict, Any, List
from sqlalchemy.orm import Session
from src.infrastructure.core.models import Adult
from src.infrastructure.database.init_db import create_table


class AdultRepo:

    @staticmethod
    def insert(conn: Session, obj: Adult) -> None:
        conn.add(obj)
        conn.commit()
        conn.refresh(obj)
        conn.close()

    @staticmethod
    def insert_bulk(conn: Session, data: List[Dict[str, Any]]) -> None:
        conn.bulk_insert_mappings(
            Adult,
            data
        )
        conn.commit()
        conn.close()

    @staticmethod
    def check_data_in_db(engine) -> bool:
        if engine.dialect.has_table(engine.connect(), 'adult'):
            return True
        # cria database aqui
        create_table(engine)
        return False
