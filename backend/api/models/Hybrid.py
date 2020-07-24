from models.ContentExtract import *
from models.CustomSVD import *


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Hybrid:
    def __init__(self, content, model, memory):
        self.content = content
        self.model = model
        self.memory = memory

    def recommend(self, user_id, topn=10, filters={}):
        #0.8 content_w + 0.2 geo_w
        cb_rec = self.content.content_recommend(user_id=user_id, top_n=topn, filters=filters)

        #0.8 cf_w + 0.2 geo_w
        model_rec = self.model.transform(user_id=user_id, topn=topn, filters=filters)

        #1 mem_w
        memory_rec = self.memory.recommend(user_id=user_id, topn=topn)



    #0.6 CB  + 0.2 FriendList + 0.2 SVD
