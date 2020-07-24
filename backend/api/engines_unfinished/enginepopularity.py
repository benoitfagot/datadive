from models.Popularity import *
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnginePopularity:

    def __init__(self, business_df, ye, review_df, checkin_df):
        self.pop = Popularity(business_df=business_df, ye=ye, review_df=review_df, checkin_df=checkin_df)

    def recommend(self, top_n=50, filters={}, elastic=True):
        return self.pop.recommend(top_n=top_n, filters=filters, elastic=elastic)