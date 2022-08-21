from django.db import models
from uuid import uuid4
from django.core.validators import MinValueValidator, MaxValueValidator


class CensusEtl(models.Model):
    GENDER = (('male', 'male'), ('female', 'female'))
    id = models.UUIDField(primary_key=True, default=uuid4)
    age = models.IntegerField(validators=[MinValueValidator(
        1), MaxValueValidator(100)], null=True, blank=True)
    workclass = models.CharField(max_length=80, null=True, blank=True)
    sex = models.CharField(max_length=10, choices=GENDER,
                           null=True, blank=True)
