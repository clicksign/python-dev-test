from pydantic import BaseModel, validator
from src.domain.process.models.enum import (
    WorkclassEnum,
    EducationEnum,
    MaritalStatusEnum,
    OccupationEnum,
    RelationshipEnum,
    RaceEnum,
    SexEnum,
    NativeCountryEnum,
    ClassFieldEnum
)


class AdultModel(BaseModel):

    '''
    Model Pydantic que faz validacoes do tipo dos dados e transformacoes que serao necessarias.
    As models nao precisam representar exatamente o banco de dados, podemos usar models por caso de usuario.
    '''
    
    age : int
    workclass : WorkclassEnum
    fnlwgt : int
    education : EducationEnum
    education_num : int
    marital_status : MaritalStatusEnum
    occupation : OccupationEnum
    relationship : RelationshipEnum
    race : RaceEnum
    sex : SexEnum
    capital_gain : int
    capital_loss : int
    hours_per_week : int
    native_country : NativeCountryEnum
    class_field : ClassFieldEnum

    @validator('age', 'fnlwgt', 'education_num', 'capital_gain', 'capital_loss', 'hours_per_week', pre=True)
    def check_is_int(cls, v):
        
        '''
        Validator para transformar os dados invalidos para 0
        '''
        
        if isinstance(v, str) and not v.isdigit():
            return 0
        return v

    class Config:
        orm_mode = True