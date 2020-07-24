import re
import pandas as pd
import numpy as np

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import pairwise_distances


class FriendSim:

    def __init__(self, ye):
        self.ye = ye

    def find_n_neighbours(self, df, nrows, n):
        if n > nrows:
            n = nrows
        order = np.argsort(df.values, axis=1)[:, :n]
        df = df.apply(lambda x: pd.Series(x.sort_values(ascending=False)
                                          .iloc[:n].index,
                                          index=['top{}'.format(i) for i in range(1, n + 1)]), axis=1)
        return df

    def standardize(self, row):
        new_row = (row - row.mean()) / (row.max() - row.min())
        return new_row

    def get_active_user(self, review_df, n_active=10):
        user_review_df_count = review_df.groupby("user_id").size()
        user_review_active_values = user_review_df_count[user_review_df_count >= n_active].reset_index()[
            "user_id"].values
        user_active_df = review_df[review_df["user_id"].isin(user_review_active_values)]
        user_active_df = user_active_df.groupby(["user_id", "business_id"], as_index=False).mean()
        return user_active_df

    def recommend(self, user_id, topn=50):

        top_recommendation = pd.DataFrame()

        # Get friend of the user in the parameter
        mustArray = [
            self.ye.bodySingleMatch("user_id", user_id)
        ]
        include_list = ["name", "user_id", "friends", "yelping_since", "review_count",
                        "average_stars", "elite"]
        user_fisrt_chunk = self.ye.getComplexeQuery(index="yelp-user*",
                                                    mustArray=mustArray, filterArray=[],
                                                    include=include_list, size=2000)

        print("Total reviews retrieved: %d" % (user_fisrt_chunk["hits"]["total"]["value"]))
        user_df = self.ye.getResultScrolling(user_fisrt_chunk)
        friends_str = user_df["friends"].values[0]

        if friends_str:
            friends_list = np.unique(friends_str.split(", "))
            print("This user has %d friends" % (len(friends_list)))
            friends_list = np.append(friends_list, np.array(user_id))
            mustArray = [
                self.ye.bodyMultivalueTerm("user_id.keyword", friends_list.tolist()),
                self.ye.bodyRange("date", gteValue="2016-01-01", lteValue="2018-12-31")

            ]
            review_fisrt_chunk = self.ye.getComplexeQuery(index="yelp-review*",
                                                          mustArray=mustArray, filterArray=[],
                                                          include=["user_id", "business_id", "stars"], size=2000)

            review_df = self.ye.getResultScrolling(review_fisrt_chunk)
            user_active_df = self.get_active_user(review_df=review_df)

            # check if user is still in user_active_df (cold start)
            if (any(user_active_df.user_id == user_id) == False):
                return pd.DataFrame()

            # retrieve all businesses seen by user to not recommend them later
            businesses_seen_by_user = user_active_df[user_active_df.user_id == user_id].business_id.unique()

            # ************ old method *****************
            Mean = user_active_df.groupby(by="user_id", as_index=False)['stars'].mean()
            Mean.rename(columns={'stars': 'mean'}, inplace=True)
            Rating_avg = pd.merge(user_active_df, Mean, on='user_id')
            Rating_avg['adg_rating'] = Rating_avg['stars'] - Rating_avg['mean']

            # Building matrix by pivot table
            rating_mat = pd.pivot_table(
                data=Rating_avg,
                index="user_id",
                columns="business_id",
                values="stars",
                fill_value=0)

            rating_standardized = rating_mat.fillna(rating_mat.mean(axis=0))

            #             #********** new method ****************
            #             #Building matrix by pivot table
            #             rating_mat = pd.pivot_table(
            #                 data=user_active_df,
            #                 index="user_id",
            #                 columns="business_id",
            #                 values="stars",
            #                 fill_value=0)
            #             rating_standardized = rating_mat.apply(self.standardize)

            nrows = rating_standardized.shape[0]
            item_similarity = cosine_similarity(rating_standardized)
            np.fill_diagonal(item_similarity, 0)
            item_similarity_df = pd.DataFrame(item_similarity, index=rating_mat.index, columns=rating_mat.index)
            sim_user_30_m = self.find_n_neighbours(item_similarity_df, nrows, 30)

            ###computing scores for each unseen business by user
            business_user = Rating_avg.groupby(by='user_id')['business_id'].apply(lambda x: ','.join(x))
            a = sim_user_30_m[sim_user_30_m.index == user_id].values
            b = a.squeeze().tolist()
            d = business_user[business_user.index.isin(b)]
            l = ','.join(d.values)
            businesses_seen_by_similar_users = l.split(',')
            businesses_under_consideration = list(
                set(businesses_seen_by_similar_users) - set(list(map(str, businesses_seen_by_user))))
            # businesses_under_consideration = list(map(int, businesses_under_consideration))
            score = []
            for item in businesses_under_consideration:
                c = rating_standardized.loc[:, item]
                d = c[c.index.isin(b)]
                f = d[d.notnull()]
                avg_user = Mean.loc[Mean['user_id'] == user_id, 'mean'].values[0]
                index = f.index.values.squeeze().tolist()
                corr = item_similarity_df.loc[user_id, index]
                fin = pd.concat([f, corr], axis=1)
                fin.columns = ['adg_score', 'correlation']
                fin['score'] = fin.apply(lambda x: x['adg_score'] * x['correlation'], axis=1)
                nume = fin['score'].sum()
                deno = fin['correlation'].sum()
                final_score = avg_user + (nume / deno)
                score.append(final_score)
            data = pd.DataFrame({'business_id': businesses_under_consideration, 'score': score})
            top_recommendation = data.sort_values(by='score', ascending=False)
            ###noralize score between 0 and 1
            top_recommendation = top_recommendation.apply(
                lambda x: (x - min(x)) / (max(x) - min(x)) if x.name == 'score' else x)
        return top_recommendation.head(topn)
