from sqlalchemy import Column, Integer, Enum
from src.config.database import base_entity
from src.domain.process.models.enum import (
    WorkclassEnum,
    ClassFieldEnum,
    EducationEnum,
    MaritalStatusEnum,
    NativeCountryEnum,
    OccupationEnum,
    RaceEnum,
    RelationshipEnum,
    SexEnum
)

class Adult(base_entity):

    '''
    Entidade que reflete a tabela `adults` no banco de dados
    '''

    __tablename__ = "adults"

    id = Column('id',Integer, primary_key=True, autoincrement=True)
    age = Column('age', Integer, nullable=False)
    workclass = Column('workclass', Enum(WorkclassEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    fnlwgt = Column('fnlwgt', Integer, nullable=False)
    education = Column('education', Enum(EducationEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    education_num = Column('education-num', Integer, nullable=False)
    marital_status = Column('marital-status', Enum(MaritalStatusEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    occupation = Column('occupation', Enum(OccupationEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    relationship = Column('relationship', Enum(RelationshipEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    race = Column('race', Enum(RaceEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    sex = Column('sex', Enum(SexEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    capital_gain = Column('capital-gain', Integer, nullable=False)
    capital_loss = Column('capital-loss', Integer, nullable=False)
    hours_per_week = Column('hours-per-week', Integer, nullable=False)
    native_country = Column('native-country', Enum(NativeCountryEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)
    class_field = Column('class', Enum(ClassFieldEnum, values_callable=lambda obj: [e.value for e in obj]), nullable=False)