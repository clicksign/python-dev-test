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
    fnlwgt = models.IntegerField(null=True, blank=True)
    education = models.CharField(max_length=40, null=True, blank=True)
    education_num = models.IntegerField(null=True, blank=True)
    marital_status = models.CharField(max_length=40, null=True, blank=True)
    occupation = models.CharField(max_length=40, null=True, blank=True)
    relationship = models.CharField(max_length=40, null=True, blank=True)
    race = models.CharField(max_length=40, null=True, blank=True)
    capital_gain = models.IntegerField(null=True, blank=True)
    capital_loss = models.IntegerField(null=True, blank=True)
    hours_per_week = models.IntegerField(null=True, blank=True)
    native_country = models.CharField(max_length=40, null=True, blank=True)
    class_category = models.CharField(max_length=10, null=True, blank=True)


class Counter(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    counter = models.IntegerField(null=True, blank=True)
