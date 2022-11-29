from sqlalchemy import VARCHAR, Column, Integer, BigInteger
from src.infrastructure.database import Base


class Adult(Base):

    __tablename__ = 'adult'

    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    age = Column('age', Integer, index=True, nullable=True)
    workclass = Column('workclass', VARCHAR(50), index=True, nullable=False)
    fnlwgt = Column('fnlwgt', Integer, index=True, nullable=True)
    education = Column('education', VARCHAR(50), index=True, nullable=False)
    education_num = Column('education-num', Integer, index=True, nullable=True)
    marital_status = Column('marital-status', VARCHAR(50), index=True, nullable=False)
    occupation = Column('occupation', VARCHAR(50), index=True, nullable=False)
    relationship = Column('relationship', VARCHAR(50), index=True, nullable=False)
    race = Column('race', VARCHAR(50), index=True, nullable=False)
    sex = Column('sex', VARCHAR(50), index=True, nullable=False)
    capital_gain = Column('capital-gain', Integer, index=True, nullable=True)
    capital_loss = Column('capital-loss', Integer, index=True, nullable=True)
    hours_per_week = Column('hours-per-week', Integer, index=True, nullable=True)
    native_country = Column('native-country', VARCHAR(50), index=True, nullable=False)
    class_ = Column('class', VARCHAR(50), index=True, nullable=False)
