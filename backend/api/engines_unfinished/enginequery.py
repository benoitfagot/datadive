import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EngineQuery:
    def __init__(self, ye):
        self.ye = ye

    def get_business_info(self, business_id):
        first_chunk = self.ye.getSingleTerm("yelp-business", "business_id.keyword", value=business_id)
        business_df = self.ye.getResultScrolling(first_chunk)
        return business_df

