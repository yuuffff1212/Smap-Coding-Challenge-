from datetime import datetime as dt
import os

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.timezone import make_aware
import numpy as np
import pandas as pd

from consumption.models import User, Consumption, Area, Tariff


class Command(BaseCommand):
    help = 'import data'

    def __init__(self):
        self.user_data_path = os.path.join(os.path.dirname(settings.BASE_DIR), 'data', "user_data.csv")
        self.consumption_data_path = os.path.join(os.path.dirname(settings.BASE_DIR), "data", "consumption/")

    def handle(self, *args, **options):
        self.import_user_data()
        self.import_consumption_data()

    def import_user_data(self):
        """Import user data into the DB"""
        user_data_df = pd.read_csv(self.user_data_path, chunksize=100,
                                   dtype={"id": np.int64, "area": np.object, "tariff": np.object})
        for row in user_data_df:
            area_list = Area.objects.filter(name=row["area"])
            tariff_list = Tariff.objects.filter(name=row["tariff"])
            if len(area_list) == 0 or len(tariff_list) == 0:
                continue
            area = area_list.pop()
            tariff = tariff_list.pop()
            User.objects.create(id=row["id"].item(), area_id=area.id, tariff=tariff.id)

    def import_consumption_data(self):
        """Import Consumption data into the DB"""
        filelist = []
        files = os.listdir(self.consumption_data_path)
        consumption_data_table = {}
        for filename in files:
            if os.path.isfile(os.path.join(self.consumption_data_path, filename)):
                consumption_data_df = pd.read_csv(files, filename, chunksize=1000, header=0, engine="python",
                                                  dtype={"datetime": np.datetime64, "consumption": np.float64})
                consumption_data_df["user_id"] = int(os.path.splitext(filename)[0])
                filelist.append(consumption_data_df[["user_id", "datetime", "consumption"]])
                consumption_data = pd.concat(filelist)

                for row in consumption_data.itertuples():
                    aware_time = make_aware(dt.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S"))
                    consumption_data_table = Consumption(user=row["user_id"], datetime=aware_time, consumption=["consumption"])
                    Consumption.objects.create(consumption_data_table)
