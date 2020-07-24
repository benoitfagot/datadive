import pandas as pd
import numpy as np

from models.Geolocation import *

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import time


class EngineGeo:

    def __init__(self, business_df):
        self.geoloc = Geolocation(business_df=business_df)
        self.geoloc.fit()

    def reset(self):
        self.geoloc.reset()

    def get_business_nearby(self):
        print(self.geoloc.get_current_location())
        return self.geoloc.get_business_nearby()

    def get_current_location(self):
        return self.geoloc.get_current_location()

    def recommend(self, lookup="", engine=True, rec_range=5):
        self.geoloc.get_neighbors_recommend(lookup=lookup, engine=engine, rec_range=rec_range)