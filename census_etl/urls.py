from django.urls import path
from .views import CensusEtlViewSet, CounterView

urlpatterns = [
    path('', CensusEtlViewSet.as_view()),
    path('counter', CounterView.as_view())
]
