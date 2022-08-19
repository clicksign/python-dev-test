from rest_framework import serializers
from .models import CensusEtl


class CensusEtlSerializer(serializers.ModelSerializer):
    class Meta:
        model = CensusEtl
        fields = '__all__'
