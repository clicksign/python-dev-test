from django.contrib import admin

from .models import CensusEtl, Counter


@admin.register(CensusEtl)
class CensusEtlAdmin(admin.ModelAdmin):
    list_display = ('id',)


@admin.register(Counter)
class Counter(admin.ModelAdmin):
    list_display = ('id',)
