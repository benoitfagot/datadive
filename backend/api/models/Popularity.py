import numpy as np
import pandas as pd


class Popularity:

    def __init__(self, business_df, ye, review_df, checkin_df):
        self.business_df = business_df
        self.ye = ye
        self.review_df = review_df
        self.checkin_df = checkin_df

    def get_model_name(self):
        return self.MODEL_NAME

    def normalize(self, df):
        dataNorm = ((df - df.min()) / (df.max() - df.min()))
        return dataNorm

    def recommend(self, top_n=50, filters={}, geo_w=0.2, pop_w=0.8, elastic=True):
        mask = np.array([])
        recommendations_df = pd.DataFrame()
        nearby_df = pd.DataFrame()
        if filters:
            if "nearby" in filters and not filters["nearby"].empty:
                nearby_df = filters["nearby"]
                nearby_ids = nearby_df["business_id"].values
                if mask.size == 0:
                    mask = nearby_ids
                else:
                    mask = np.intersect1d(mask, nearby_ids)

            if "categories" in filters and filters["categories"]:
                mask_cat = self.business_df[self.business_df["categories"].str.contains(filters["categories"])][
                    "business_id"].values
                mask = np.intersect1d(mask, mask_cat)

            # review_df = self.review_df[self.review_df["business_id"].isin(mask)]
            if mask.size > 0:
                if elastic:
                    mustArray = [
                        self.ye.bodyMultivalueTerm("business_id.keyword", np.unique(mask).tolist()),
                        self.ye.bodyRange("date", gteValue="2016-01-01", lteValue="2018-12-31")
                    ]
                    review_fisrt_chunk = self.ye.getComplexeQuery(index="yelp-review*",
                                                                  mustArray=mustArray, filterArray=[],
                                                                  include=["business_id", "user_id", "stars"],
                                                                  size=2000)

                    print("Total reviews retrieved: %d" % (review_fisrt_chunk["hits"]["total"]["value"]))
                    review_df = self.ye.getResultScrolling(review_fisrt_chunk)
                else:
                    review_df = self.review_df[self.review_df["business_id"].isin(mask)]

                recommendations_df = review_df.groupby("business_id")["stars"] \
                    .agg(["sum", "count"]) \
                    .reset_index()

                recommendations_df["ratings_avg"] = (recommendations_df["sum"] / recommendations_df["count"])
                recommendations_df = recommendations_df \
                    .sort_values(ascending=False, by="ratings_avg") \
 \
                    # Normalization of popularity score
                recommendations_df["score_rating"] = self.normalize(recommendations_df["ratings_avg"])

                # Join df above and checkins
                recommendations_df = pd.merge(left=recommendations_df, right=self.checkin_df, how="inner",
                                              on="business_id")

                # Normalization of popularity score
                recommendations_df["score_checkin"] = self.normalize(recommendations_df["all_years"])

                recommendations_df["pop_score"] = (0.5 * recommendations_df["score_rating"]) + (
                            0.5 * recommendations_df["score_checkin"])
                # print()

                if not nearby_df.empty:
                    # Join geolocation and rating normalized score
                    recommendations_df = pd.merge(left=recommendations_df, right=nearby_df, how="inner",
                                                  on="business_id")

                    # recommendations_df = recommendations_df.rename(
                    #    columns={"score": "pop_score"})

                    # recommendations_df["geo_score"] = recommendations_df["geo_score"] * geo_w
                    # recommendations_df["pop_score"] = recommendations_df["pop_score"] * pop_w
                    recommendations_df["score"] = recommendations_df["geo_score"] * geo_w + recommendations_df[
                        "pop_score"] * pop_w

                    recommendations_df = recommendations_df.sort_values(ascending=False, by="score")

                recommendations_df = recommendations_df.head(top_n)

        return recommendations_df