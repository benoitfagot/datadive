from models.ContentExtract import *

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EngineCB:

    def __init__(self, ye, business_df):
        logger.info("Initilizing Content Based Engine")
        self.content = ContentExtact(ye=ye, business_df=business_df)

    def load(self):
        self.content.load()

    def train_word2vec(self):
        self.train_word2vec()

    def train_texttovec(self):
        self.content.text_to_vec()

    def keyword_recommend(self, input_str, top_n=10, filters={}):
        return self.content.keyword_recommend(input_str=input_str, top_n=top_n, filters=filters)

    def content_recommend(self, user_id, top_n=10, filters={}):
        return self.content.content_recommend(user_id=user_id, top_n=top_n, filters=filters)