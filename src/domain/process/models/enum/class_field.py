from enum import Enum


class ClassFieldEnum(Enum):
   
   '''
   Enum usado para transcrever as possibilidades de dados de cada campo
   '''
   
   MORE_THAN = ">50K"
   LASS_OR_EQUAL_THAN = "<=50K"
   EMPTY_VALUE = "?"
