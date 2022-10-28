from enum import Enum


class ColumnNamesEnum(Enum):
    AGE = "age"
    WORKCLASS = "workclass"
    FNLWGT = "fnlwgt"
    EDUCATION = "education"
    EDUCATION_NUM= "education_num"
    MARITAL_STATUS= "marital_status"
    OCCUPATION = "occupation"
    RELATIONSHIP = "relationship"
    RACE = "race"
    SEX = "sex"
    CAPITAL_GAIN = "capital_gain"
    CAPITAL_LOSS= "capital_loss"
    HOURS_PER_WEEK= "hours_per_week"
    NATIVEE_COUNTRY= "native_country"
    CLASS_FIELD = "class_field"

    @classmethod
    def as_list(self) -> list:
        return [e.value for e in self]