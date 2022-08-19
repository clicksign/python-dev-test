from django.contrib import admin

from .models import CensusEtl


@admin.register(CensusEtl)
class CensusEtlAdmin(admin.ModelAdmin):
    list_display = ('id',)
