import datetime
import os
import logging

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.timezone import make_aware
import numpy as np
import pandas as pd

from consumption.models import User, Consumption, Area, Tariff


class Command(BaseCommand):
    help = 'import data'

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

    def handle(self, *args, **options):
        self.import_user_data(os.path.join(os.path.dirname(settings.BASE_DIR), 'data', "user_data.csv"))
        self.import_consumption_data(os.path.join(os.path.dirname(settings.BASE_DIR), "data", "consumption/"))

    def import_user_data(self, user_data_path):
        """Import user data into the DB"""

        # Get nameObject in Area model
        areas = Area.objects.all()
        areas_val = areas.values()
        # Get nameObject in Tariff Model
        tariffs = Tariff.objects.all()
        tariffs_val = tariffs.values()

        try:
            user_data_df = pd.read_csv(user_data_path, chunksize=100,
                                       dtype={"id": np.int64, "area": np.object, "tariff": np.object})
            for row in user_data_df:
                if row["area"] in areas_val["name"]:
                    [row["area"]] = Area.objects.get(pk=area_id)
                if row["tariff"] in tariffs_val:
                    tariff[row["tariff"]] = Tariff.objects.get(pk=user_tariffs.id)
                user_table = User(id=row["id"], area=[row["area"]], tariff=tariff[row["tariff"]])
                User.objects.bulk_create(user_table)
                self.logger.info("Completely user data imported")
        except Exception:
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
        except Exception:
            self.logger.error("Consumption data already imported")
