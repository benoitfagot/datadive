import pandas as pd
import numpy as np
import time

from sklearn.preprocessing import LabelEncoder
from sklearn.decomposition import NMF
from scipy import sparse

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomNMF:
    def __init__(self, review_df):
        self.user_active_df = self.get_active_user(review_df)

    def get_active_user(self, review_df):
        n_active = 40
        user_review_df_count = review_df.groupby("user_id").size()
        user_review_active_values = user_review_df_count[user_review_df_count >= n_active].reset_index()[
            "user_id"].values
        user_active_df = review_df[review_df["user_id"].isin(user_review_active_values)]
        user_active_df = user_active_df.groupby(["user_id", "business_id"], as_index=False).mean()
        return user_active_df

    def create_utility_matrix(self, df):
        user_encoder = LabelEncoder()
        business_encoder = LabelEncoder()

        user_ids = pd.DataFrame(columns=["user_id_matrix"])
        business_ids = pd.DataFrame(columns=["business_id_matrix"])
        df["user_id_matrix"] = user_encoder.fit_transform(df['user_id'])
        df["business_id_matrix"] = business_encoder.fit_transform(df['business_id'])

        user_ids["user_id_matrix"] = df["user_id_matrix"].unique()
        business_ids["business_id_matrix"] = df["business_id_matrix"].unique()
        user_ids["user_id"] = user_encoder.inverse_transform(user_ids["user_id_matrix"])
        business_ids["business_id"] = business_encoder.inverse_transform(business_ids["business_id_matrix"])

        return df, user_ids, business_ids

    def create_sparse_matrix(self, user_active_matrix, user_ids, business_ids):
        highest_user_id = len(user_ids['user_id'].unique())
        highest_business_id = len(business_ids['business_id'].unique())
        shape_matrix = (highest_user_id, highest_business_id)
        ratings_mat = sparse.lil_matrix(shape_matrix)
        for i, row in user_active_matrix.iterrows():
            ratings_mat[row["user_id_matrix"], row["business_id_matrix"]] = row["stars"]
        return ratings_mat

    def nmf_pred(self, ratings_mat, n_factor=20):
        nmf = NMF(n_components=n_factor)
        nmf.fit(ratings_mat)
        #Features
        W = nmf.transform(ratings_mat)
        #Features Weight
        H = nmf.components_
        #Reconstructed matrix
        ratings_mat_fitted = W.dot(H)
        #Error
        self.error = nmf.reconstruction_err_

        return ratings_mat_fitted

    def fit(self, n_factor=50):
        logger.info("Start building matrix")
        start = time.time()
        self.cf_preds_df = None

        user_active_matrix, self.user_ids, self.business_ids = self.create_utility_matrix(self.user_active_df)
        rating_mat = self.create_sparse_matrix(user_active_matrix, self.user_ids, self.business_ids)
        self.cf_preds_df = self.nmf_pred(ratings_mat=rating_mat, n_factor=n_factor)

        logger.info("Matrix built in %s seconds." % (time.time() - start))

    def transform(self, user_id, filters={}, cf_w=0.8, geo_w=0.2, topn=1000):
        logger.info("Start predicting")
        start = time.time()
        recommendations_df = pd.DataFrame()

        user_id_num = self.user_ids[self.user_ids["user_id"] == user_id].user_id_matrix.values
        if user_id_num.size > 0:
            user_id_num = user_id_num.astype(int)[0]
            pred_user = self.cf_preds_df[user_id_num, :]
            sorted_user_predictions = pd.DataFrame(pred_user.T, columns=["score"])
            sorted_user_predictions = pd.merge(sorted_user_predictions, self.business_ids, left_index=True,
                                               right_on="business_id_matrix") \
                .drop(columns=["business_id_matrix"]) \
                .sort_values(ascending=False, by="score") \
                .reset_index(drop=True)

            items_to_ignore = self.user_active_df[self.user_active_df["user_id"] == user_id]
            items_to_ignore = items_to_ignore["business_id"].unique()

            # Recommend the highest predicted rating movies that the user hasn't seen yet.
            recommendations_df = sorted_user_predictions[~sorted_user_predictions['business_id'].isin(items_to_ignore)] \
                .sort_values('score', ascending=False)

            if "nearby" in filters and not filters["nearby"].empty:
                nearby_df = filters["nearby"]
                recommendations_df = pd.merge(left=recommendations_df, right=nearby_df, how="inner", on="business_id")
                if not recommendations_df.empty:
                    recommendations_df = recommendations_df.rename(columns={"score": "cf_score"})
                    recommendations_df["score"] = cf_w * recommendations_df["cf_score"] + geo_w * recommendations_df[
                        "geo_score"]
                    recommendations_df = recommendations_df.sort_values(by="score", ascending=False)

            recommendations_df = recommendations_df.head(topn)

            logger.info("Prediction done in %s seconds." % (time.time() - start))
            return items_to_ignore, recommendations_df
        else:
            logger.info("This user %s is not exist" % (user_id))
            return np.array([]), recommendations_df

    def get_error(self):
        return self.error