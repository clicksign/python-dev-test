from rest_framework import serializers
from .models import CensusEtl
from rest_framework.validators import UniqueTogetherValidator


# class CensusEtlSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = CensusEtl
#         fields = '__all__'


class CensusEtlBulkCreateUpdateSerializer(serializers.ListSerializer):
    def create(self, validated_data):
        census_data = [CensusEtl(**item) for item in validated_data]
        return CensusEtl.objects.bulk_create(census_data)


class CensusEtlSerializer(serializers.ModelSerializer):
    class Meta:
        model = CensusEtl
        fields = [
            'age',
            'workclass',
            'fnlwgt',
            'education',
            'education_num',
            'marital_status',
            'occupation',
            'relationship',
            'race',
            'sex',
            'capital_gain',
            'capital_loss',
            'hours_per_week',
            'native_country',
            'class_category'
        ]
        validators = [
            UniqueTogetherValidator(
                queryset=model.objects.all(),
                fields=[
                    'age',
                    'workclass',
                    'fnlwgt',
                    'education',
                    'education_num',
                    'marital_status',
                    'occupation',
                    'relationship',
                    'race',
                    'sex',
                    'capital_gain',
                    'capital_loss',
                    'hours_per_week',
                    'native_country',
                    'class_category'
                ],
                message='Entry is already in the database.'
            )
        ]
        list_serializer_class = CensusEtlBulkCreateUpdateSerializer
