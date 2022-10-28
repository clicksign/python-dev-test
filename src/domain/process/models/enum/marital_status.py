from enum import Enum


class MaritalStatusEnum(Enum):
    MARRIED_CIV_SPOUSE = "Married-civ-spouse"
    DIVORCED = "Divorced"
    NEVER_MARRIED = "Never-married"
    SEPARATED = "Separated"
    WIDOWED = "Widowed"
    MARRIED_SPOUSE_ABSENT = "Married-spouse-absent"
    MARRIED_AF_SPOUSE = "Married-AF-spouse"
    EMPTY_VALUE = "?"