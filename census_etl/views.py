from rest_framework import generics
from .models import CensusEtl
from .serializers import CensusEtlSerializer


class CensusEtlViewSet(generics.ListCreateAPIView):
    queryset = CensusEtl.objects.all()
    serializer_class = CensusEtlSerializer
