from enum import Enum


class WorkclassEnum(Enum):
    PRIVATE = "Private"
    SELF_EMP_N_INC = "Self-emp-not-inc"
    SELF_EMP_INC = "Self-emp-inc"
    FEDERAL_GOV = "Federal-gov"
    LOCAL_GOV = "Local-gov"
    STATE_GOV = "State-gov"
    WITHOUR_PAY= "Without-pay"
    NEVER_WORKED= "Never-worked"
    EMPTY_VALUE = "?"