from django.urls import path
from .views import CensusEtlViewSet

urlpatterns = [
    path('', CensusEtlViewSet.as_view())

]
