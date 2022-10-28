from enum import Enum


class RelationshipEnum(Enum):
   WIFE = "Wife"
   OWN_CHILD = "Own-child"
   HUSBAND = "Husband"
   NOT_IN_FAMILY = "Not-in-family"
   OTHER_RELATIVE = "Other-relative"
   UNMARRIED = "Unmarried"
   EMPTY_VALUE = "?"