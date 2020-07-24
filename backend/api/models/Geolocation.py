import pandas as pd
import numpy as np
import time


from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from geopy.geocoders import Nominatim
from math import radians, cos, sin, asin, sqrt, pi
import requests
from sklearn.neighbors import BallTree

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Geolocation:
    token = "5e599797dbecfc222d30063da4b86640"
    send_url = "http://api.ipstack.com/check?access_key=" + token

    def __init__(self, business_df):
        self.business_df = business_df
        self.location = {}
        self.distance_df = pd.DataFrame()
        self.geolocator = Nominatim(user_agent="Data_Dive_Prod")

    def deg2rad(self, degree):
        '''
        function to convert degree to radian
        '''
        rad = degree * (pi / 180)
        return (rad)

    def fit(self):
        coordinates = self.business_df[["latitude", "longitude"]].apply(self.deg2rad)
        self.tree = BallTree(coordinates, metric='haversine')

    def normalize(self, df):
        dataNorm = ((df - df.min()) / (df.max() - df.min()))
        return dataNorm

    def reset(self):
        self.location = {}
        self.distance_df = pd.DataFrame()

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formule
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers.
        res = c * r
        return np.round(res, 4)

    def show_current_location(self):
        print("Current location: (Lat: %s, Long: %s)" % (self.location["latitude"], self.location["longitude"]))

    def get_current_location(self):
        return self.location

    def get_business_nearby(self):
        return self.distance_df

    def get_info_coordinate(self, coordinate):
        gps = str(coordinate["latitude"]) + ',' + str(coordinate["longitude"])
        location = self.geolocator.reverse(gps)
        self.location["city"] = location.raw["address"]["city"]
        self.location["country"] = location.raw["address"]["country"]
        self.show_current_location()

    def get_coordinate_address(self, address):
        location = None
        try:
            location = self.geolocator.geocode(address)
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
            time.sleep(1)
            try:
                location = self.geolocator.geocode(address)
            except (
                    GeocoderTimedOut, GeocoderServiceError,
                    GeocoderUnavailable):
                # logger.info('GeocoderServiceError occored')
                return None, None

        # logger.info(location)
        if location:
            self.location["latitude"] = location.latitude
            self.location["longitude"] = location.longitude
            coordinate = {"latitude": location.latitude,
                          "longitude": location.longitude}
            self.get_info_coordinate(coordinate=coordinate)

    def get_coordinate_ip_address(self):
        geo_req = requests.get(self.send_url)
        response = geo_req.json()
        self.location["latitude"] = float(response["latitude"])
        self.location["longitude"] = float(response["longitude"])
        self.location["city"] = response["city"]
        self.location["country"] = response["country_name"]
        self.show_current_location()

    def get_neighbors_recommend(self, lookup="", engine=True, rec_range=5):

        if engine:
            if not lookup:
                self.get_coordinate_ip_address()
            else:
                self.get_coordinate_address(lookup)
        else:
            self.location = lookup
            self.get_info_coordinate(coordinate=lookup)

        input_point = [[self.deg2rad(self.location["latitude"]), self.deg2rad(self.location["longitude"])]]
        nearest_point = self.tree.query_radius(input_point, r=rec_range / 6371)[0]

        self.distance_df = pd.DataFrame()
        self.distance_df = self.business_df[["business_id", "longitude", "latitude"]]
        self.distance_df = self.business_df[self.business_df.index.isin(nearest_point)]
        if not self.distance_df.empty:
            self.distance_df["distance"] = self.distance_df.apply(
                lambda x: self.haversine_distance(self.location["latitude"], self.location["longitude"], x["latitude"],
                                                  x["longitude"]), axis=1)

            self.distance_df = self.distance_df.sort_values(ascending=True, by="distance").reset_index(drop=True)
            self.distance_df = self.distance_df[["business_id", "distance"]]

            # Normalization
            self.distance_df["geo_score"] = self.normalize(self.distance_df["distance"])
            self.distance_df["geo_score"] = 1 - self.distance_df["geo_score"]