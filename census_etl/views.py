from rest_framework import generics
from .models import CensusEtl
from .serializers import CensusEtlSerializer


class CensusEtlViewSet(generics.ListCreateAPIView):
    queryset = CensusEtl.objects.all()
    serializer_class = CensusEtlSerializer

    def get_serializer(self, *args, **kwargs):
        if isinstance(kwargs.get("data", {}), list):
            kwargs["many"] = True
        return super(CensusEtlViewSet, self).get_serializer(*args, **kwargs)
