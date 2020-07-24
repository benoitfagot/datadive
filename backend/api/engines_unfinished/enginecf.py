import pickle
from utilcheck import *
from models.CustomSVD import *
from models.FriendSim import *

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

data_path = "/home/hongphuc95/notebookteam/dataset/"
api_path = "/home/hongphuc95/notebookteam/api/"


class EngineCF:

    def __init__(self, business_df, review_df, ye):
        logger.info("Initilizing Collaborative Filtering Engine")
        self.business_df = business_df
        self.ye = ye
        self.review = review_df
        self.model = CustomSVD(review_df=review_df)
        #self.memory = FriendSim(ye=ye)

    def train(self, n_factor=20):
        self.model.fit(n_factor=n_factor)

    def predict(self, user_id, filters={}, model_w=0.4, memory_w=0.6, topn=20):
        already_reviewed, pred_model = self.model.transform(user_id=user_id, filters=filters, topn=1000)
        #pred_memory = self.memory.recommend(user_id=user_id, topn=1000)

        if not pred_model.empty:
            return already_reviewed, pred_model.head(topn)
        else:
            return pd.DataFrame(), pd.DataFrame()

    def business_in_common(self, user_x, user_y, topn=5):
        business_in_common = pd.DataFrame()
        user_x_df = self.review [self.review ["user_id"] == user_x]
        user_x_df = user_x_df.groupby(["user_id", "business_id"], as_index=False)["stars"].mean()
        user_y_df = self.review [self.review ["user_id"] == user_y]
        user_y_df = user_y_df.groupby(["user_id", "business_id"], as_index=False)["stars"].mean()
        business_in_common = pd.merge(left=user_x_df, right=user_y_df, how="inner", on="business_id")
        if not business_in_common.empty:
            business_in_common.rename(columns={"user_id_x": "user_id_being_recommended",
                                               "user_id_y": "user_id_similar_user",
                                               "stars_x": "stars_user_being_recommended",
                                               "stars_y": "stars_similar_user"},
                                      inplace=True)
            business_in_common = business_in_common[
                ["business_id", "user_id_being_recommended", "user_id_similar_user", "stars_user_being_recommended",
                 "stars_similar_user"]]
        return business_in_common

    def save_model(self):
        with open(api_path + "models/customsvd.model", "wb") as f:
            pickle.dump(self.model, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_model(self):
        try:
            with open(api_path + "models/customsvd.model", "rb") as f:
                self.model = pickle.load(f)
                logger.info("Custom SVD model loaded")
        except (FileNotFoundError, IOError):
            logger.info("File not found")



