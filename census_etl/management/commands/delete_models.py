from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group
from census_etl.models import CensusEtl, Counter


class Command(BaseCommand):

    def handle(self, **options):

        CensusEtl.objects.all().delete()
        Counter.objects.all().delete()
