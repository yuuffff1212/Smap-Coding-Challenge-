from django.contrib import admin
from consumption.models import Area, Tariff, User, Consumption

# Register your models here.
admin.site.register(Area)
admin.site.register(Tariff)
admin.site.register(User)
admin.site.register(Consumption)
