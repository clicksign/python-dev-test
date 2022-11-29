from enum import Enum


class CommonType(Enum):

    @classmethod
    def values(cls) -> list:
        return [item.value for item in cls]


class WorkclassType(CommonType):
    PRIVATE = 'Private'
    SELF_EMP_NOT_INC = 'Self-emp-not-inc'
    SELF_EMP_INC = 'Self-emp-inc'
    FEDERAL_GOV = 'Federal-gov'
    LOVAL_GOV = 'Local-gov'
    STATE_GOV = 'State-gov'
    WITHOUT_PAY = 'Without-pay'
    NEVER_WORKED = 'Never-worked'


class EducationType(CommonType):
    BACHELORS = 'Bachelors'
    SOME_COLLEGE = 'Some-college'
    _11TH = '11th'
    HD_GRAD = 'HS-grad'
    PROF_SCHOOL = 'Prof-school'
    ASSOC_ACDM = 'Assoc-acdm'
    ASSOC_VOC = 'Assoc-voc'
    _9TH = '9th'
    _7TH_8TH = '7th-8th'
    _12TH = '12th'
    MASTERS = 'Masters'
    _1ST_4TH = '1st-4th'
    _10TH = '10th'
    DOCTORATE = 'Doctorate'
    _5TH_6TH = '5th-6th'
    PRESCHOOL = 'Preschool'


class MaritalstatusType(CommonType):
    MARRIED_CIV_SPOUSE = 'Married-civ-spouse'
    DIVORCED = 'Divorced'
    NEVER_MARRIED = 'Never-married'
    SEPARATED = 'Separated'
    WIDOWED = 'Widowed'
    MARRIED_SPOUSE_ABSENT = 'Married-spouse-absent'
    MARRIED_AF_SPOUSE = 'Married-AF-spouse'


class OccupationType(CommonType):
    TECH_SUPPORT = 'Tech-support'
    CRAFT_REPAIR = 'Craft-repair'
    OTHER_SERVICE = 'Other-service'
    SALES = 'Sales'
    EXEC_MANAGERIAL = 'Exec-managerial'
    PROF_SPECIALTY = 'Prof-specialty'
    HANDLERS_CLEANERS = 'Handlers-cleaners'
    MACHINE_OP_INSPCT = 'Machine-op-inspct'
    ADM_CLERICAL = 'Adm-clerical'
    FARMING_FISHING = 'Farming-fishing'
    TRANSPORT_MOVING = 'Transport-moving'
    PRIV_HOUSE_SERV = 'Priv-house-serv'
    PROTECTIVE_SERV = 'Protective-serv'
    ARMED_FORCES = 'Armed-Forces'


class RelationshipType(CommonType):
    WIFE = 'Wife'
    OWN_CHILD = 'Own-child'
    HUSBAND = 'Husband'
    NOT_IN_FAMILY = 'Not-in-family'
    OTHER_RELATIVE = 'Other-relative'
    UNMARRIED = 'Unmarried'


class RaceType(CommonType):
    WHITE = 'White'
    ASIAN_PAC_ISLANDER = 'Asian-Pac-Islander'
    AMER_INDIAN_ESKIMO = 'Amer-Indian-Eskimo'
    OTHER = 'Other'
    BLACK = 'Black'


class SexType(CommonType):
    FEMALE = 'Female'
    MALE = 'Male'


class NativecountryType(CommonType):
    UNITED_STATES = 'United-States'
    CAMBODIA = 'Cambodia'
    ENGLAND = 'England'
    PUERTO_RICO = 'Puerto-Rico'
    CANADA = 'Canada'
    GERMANY = 'Germany'
    OUTLYING_US = 'Outlying-US(Guam-USVI-etc)'
    INDIA = 'India'
    JAPAN = 'Japan'
    GREECE = 'Greece'
    SOUTH = 'South'
    CHINA = 'China'
    CUBA = 'Cuba'
    IRAN = 'Iran'
    HONDURAS = 'Honduras'
    PHILIPPINES = 'Philippines'
    ITALY = 'Italy'
    POLAND = 'Poland'
    JAMAICA = 'Jamaica'
    VIETNAM = 'Vietnam'
    MEXICO = 'Mexico'
    PORTUGAL = 'Portugal'
    IRELAND = 'Ireland'
    FRANCE = 'France'
    DOMINICAN_REPUBLIC = 'Dominican-Republic'
    LAOS = 'Laos'
    ECUADOR = 'Ecuador'
    TAIWAN = 'Taiwan'
    HAITI = 'Haiti'
    COLUMBIA = 'Columbia'
    HUNGARY = 'Hungary'
    GUATEMALA = 'Guatemala'
    NICARAGUA = 'Nicaragua'
    SCOTLAND = 'Scotland'
    THAILAND = 'Thailand'
    YUGOSLAVIA = 'Yugoslavia'
    EL_SALVADOR = 'El-Salvador'
    TRINADAD_TOBAGO = 'Trinadad&Tobago'
    PERU = 'Peru'
    HONG = 'Hong'
    HOLAND_NETHERLANDS = 'Holand-Netherlands'


class ClassType(CommonType):
    GT_50K = '>50K'
    LTE_50K = '<=50K'


class ColumnType(CommonType):
    AGE = 'age'
    WORKCLASS = 'workclass'
    FNLWGT = 'fnlwgt'
    EDUCATION = 'education'
    EDUCATION_NUM = 'education-num'
    MARITAL_STATUS = 'marital-status'
    OCCUPATION = 'occupation'
    RELATIONSHIP = 'relationship'
    RACE = 'race'
    SEX = 'sex'
    CAPITAL_GAIN = 'capital-gain'
    CAPITAL_LOSS = 'capital-loss'
    HOURS_PER_WEEK = 'hours-per-week'
    NATIVE_COUNTRY = 'native-country'
    CLASS = 'class'
