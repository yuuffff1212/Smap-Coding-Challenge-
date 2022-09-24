# standard library
from django.core.management.base import BaseCommand
from django.conf import settings
import datetime
import os
# third party library
from django.utils.timezone import make_aware
import numpy as np
import pandas as pd
# Application/library specific library
from consumption.models import User, Consumption, Area, Tariff


class Command(BaseCommand):
    help = 'import data'

    def handle(self, *args, **options):
        user_data_path = os.path.join(os.path.dirname(settings.BASE_DIR), 'data', "user_data.csv")
        consumption_data_path = os.path.join(os.path.dirname(settings.BASE_DIR), "data", "consumption/")
        self.import_user_data(user_data_path)
        self.import_consumption_data(consumption_data_path)

    def import_user_data(self, user_data_path):
        """Import user data into the DB"""
        areas_obj = {}
        tariffs_obj = {}

        # Get nameObject in Area model
        areas = Area.objects.all()
        for area in areas:
            areas_obj = area.name
        # Get nameObject in Tariff Model
        tariffs = Tariff.objects.all()
        for tariff in tariffs:
            tariffs_obj = tariff.name

        try:
            user_data_df = pd.read_csv(user_data_path, chunksize=100,
                                       dtype={"id": np.int64, "area": np.object, "tariff": np.object})
            for row in user_data_df:
                user_table = User(id=row["id"], area=areas_obj, tariff=tariffs_obj)
                User.objects.bulk_create(user_table)
                self.logger.info("Completely user data imported")
            raise ImportError
        except ImportError:
            self.logger.error("User data already imported")

    def import_consumption_data(self, consumption_data_path):
        """Import Consumption data into the DB"""
        try:
            users = User.objects.all()
            consumption_data_df = pd.read_csv(consumption_data_path, chunksize=100,
                                              dtype={"datetime": np.datetime64, "consumption": np.float64})
            for row in consumption_data_df:
                aware_time = make_aware(datetime.datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S"))
                consumption_data_table = Consumption(user=users.id, datetime=aware_time, consumption=row["consumption"])
                Consumption.objects.bulk_create(consumption_data_table)
                self.logger.info("Completely Consumption data imported")
            raise ConsumptionDataImportError
        except ConsumptionDataImportError:
            self.logger.error("Consumption data already imported")
