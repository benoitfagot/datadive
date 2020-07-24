import pandas as pd
import numpy as np
import nltk
import pickle
import time

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from utilcheck import *

from nltk.tokenize import word_tokenize
import gensim
from gensim.models import Word2Vec
from nltk.corpus import stopwords
nltk.download('stopwords')
from sklearn.metrics.pairwise import cosine_similarity

data_path = "/home/hongphuc95/notebookteam/dataset/"
api_path = "/home/hongphuc95/notebookteam/api/"


class ContentExtact:

    def __init__(self, ye, business_df):
        self.ye = ye
        self.business_df = business_df

    ##########################################################
    #                     Loading tools                      #
    ##########################################################
    def load(self):
        self.load_text_to_vec()
        self.load_word2vec()

    ##########################################################
    #                    Text Processing                     #
    ##########################################################

    def clean_text(self, text):
        all_stopwords = stopwords.words('english')
        text_tokens = word_tokenize(text.lower())
        tokens_without_sw = [word for word in text_tokens if not word in all_stopwords]
        return tokens_without_sw

    def text_processing(self):
        logger.info("Cleaning text in progress...")
        start = time.time()
        review_df = pd.read_json(data_path + "cleaned/review_cleaned_2016_2019.json", lines=True)
        review_by_business = review_df.groupby('business_id')['text'].agg(lambda col: ' '.join(col)).reset_index()
        review_by_business["text"] = review_by_business["text"].apply(lambda x: self.clean_text(x))
        logger.info("Done cleaning text in %s seconds." % (time.time() - start))
        return review_by_business

    ##########################################################
    #                        Word2Vec                        #
    ##########################################################
    def train_word2vec(self, review_by_business=None, save=True):
        logger.info("Training Word2Vec Model in progress...")
        start = time.time()
        model = gensim.models.Word2Vec(review_by_business["text"], min_count=5, size=200, workers=4)
        logger.info("Done Word2Vec text in %s seconds." % (time.time() - start))
        self.model = model
        if save:
            self.save_word2vec(self.model)

    def save_word2vec(self, model):
        model.save(api_path + "trained/review_full.model")

    def load_word2vec(self):
        try:
            self.model = Word2Vec.load(api_path + "trained/review_full.model")
            logger.info("Reviews vectorized loaded")
        except (FileNotFoundError, IOError):
            self.model = None
            logger.info("File not found")

    ##########################################################
    #                        TextToVec                       #
    ##########################################################

    def text_to_vec(self, save=True):
        logger.info("Convert texts to vectors in progress...")
        start = time.time()
        self.load_word2vec()
        review_by_business = self.text_processing()

        if not self.model:
            self.train_word2vec(review_by_business=review_by_business, save=True)

        # Clean DF
        review_by_business["text_vec"] = review_by_business["text"].apply(
            lambda x: self.avg_feature_vector(x, model=self.model, n_features=200))

        review_by_business.drop("text", axis=1, inplace=True)
        if save:
            self.save_text_to_vec(review_by_business)
        self.docvecs = review_by_business
        logger.info("Done converting texts to vectors in %s seconds." % (time.time() - start))

    def save_text_to_vec(self, review_by_business):
        with open(api_path + "trained/review_2016_2019_full_vectorized.pickle", "wb") as f:
            pickle.dump(review_by_business, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_text_to_vec(self):
        try:
            with open(api_path + "trained/review_2016_2019_full_vectorized.pickle", "rb") as f:
                self.docvecs = pickle.load(f)
                logger.info("Reviews vectorized loaded")
        except (FileNotFoundError, IOError):
            self.text_to_vec()

    def avg_feature_vector(self, sentence, model, n_features):
        index2word_set = set(model.wv.index2word)
        feature_vec = np.zeros((n_features,), dtype='float32')
        n_words = 0
        for word in sentence:
            if word in index2word_set:
                n_words += 1
                feature_vec = np.add(feature_vec, model.wv[word])
        if (n_words > 0):
            feature_vec = np.divide(feature_vec, n_words)
        return feature_vec

    ##########################################################
    #                        Recommend                       #
    ##########################################################

    def keyword_recommend(self, input_str, top_n=20, filters={}, content_w=0.8, geo_w=0.2):
        docvecs = self.docvecs
        nearby_df = pd.DataFrame()

        if filters:
            mask = np.array([])
            if "nearby" in filters and not filters["nearby"].empty:
                nearby_df = filters["nearby"]
                nearby_ids = nearby_df["business_id"].values
                if mask.size == 0:
                    mask = nearby_ids
                else:
                    mask = np.intersect1d(mask, nearby_ids)

            #if "categories" in filters and filters["categories"]:
            #    mask_cat = self.business_df[self.business_df["categories"].str.contains(filters["categories"])][
            #        "business_id"].values
            #    if mask.size == 0:
            #        mask = mask_cat
            #    else:
            #        mask = np.intersect1d(mask, mask_cat)

            docvecs = docvecs[docvecs["business_id"].isin(mask)]

        from nltk.tokenize import word_tokenize
        input_vec = pd.DataFrame({"text": [input_str]})
        input_vec["text"] = input_vec["text"].apply(lambda x: word_tokenize(x.lower()))
        input_vec["text_vec"] = input_vec["text"].apply(
            lambda x: self.avg_feature_vector(x, model=self.model, n_features=200))
        business_similarity = docvecs[["business_id", "text_vec"]]

        # compute similarity array
        business_similarity["score"] = business_similarity["text_vec"].apply(
            lambda x: cosine_similarity([x], [input_vec["text_vec"].values[0]])[0][0])
        business_similarity = business_similarity.drop(columns=["text_vec"]).sort_values(ascending=False,
                                                                                         by="score")
        #print("Shape business similarity before: %s" % (business_similarity.shape))
        if "nearby" in filters and not filters["nearby"].empty:
            business_similarity = business_similarity.rename(columns={"score": "content_score"})
            business_similarity = pd.merge(left=business_similarity, right=nearby_df, how="inner", on="business_id")
            business_similarity["score"] = content_w * business_similarity["content_score"] + geo_w * \
                                           business_similarity["geo_score"]
            business_similarity = business_similarity.sort_values(by="score", ascending=False)
            #print("Shape business similarity after: %s" % (business_similarity.shape))

        business_similarity = business_similarity.head(top_n)
        return business_similarity

    def business_similarity(self, business_ids, top_n, filters={}, content_w=0.8, geo_w=0.2):

        docvecs = self.docvecs
        businesses_similarity = pd.DataFrame()
        nearby_df = pd.DataFrame()

        if "categories" in filters and filters["categories"]:
            cat_ids = self.business_df[self.business_df["categories"].str.contains(filters["categories"])][
                "business_id"].values
            if cat_ids.size > 0:
                business_ids = np.intersect1d(business_ids, cat_ids)

        if business_ids.size > 0:
            if "nearby" in filters and not filters["nearby"].empty:
                nearby_df = filters["nearby"]
                nearby_ids = nearby_df["business_id"].values
                if nearby_ids.size > 0:
                    mask = np.append(nearby_ids, business_ids)
                    docvecs = docvecs[docvecs["business_id"].isin(mask)]

            business_ids = business_ids[:5]
            for business_id in business_ids:
                business_similarity = pd.DataFrame()
                input_vec = docvecs[docvecs["business_id"] == business_id]
                business_similarity = docvecs[["business_id", "text_vec"]]

                # compute similarity array
                business_similarity["score"] = business_similarity["text_vec"].apply(
                    lambda x: cosine_similarity([x], [input_vec["text_vec"].values[0]])[0][0])
                business_similarity = business_similarity.drop(columns=["text_vec"]).sort_values(ascending=False,
                                                                                                 by="score")

                # Filter business with the same id
                business_similarity = business_similarity[~business_similarity["business_id"].isin(business_ids)]
                business_similarity["input_business_id"] = business_id

                if "nearby" in filters and not filters["nearby"].empty:
                    business_similarity = business_similarity.rename(columns={"score": "content_score"})
                    business_similarity = pd.merge(left=business_similarity, right=nearby_df, how="inner",
                                                   on="business_id")
                    business_similarity["score"] = content_w * business_similarity["content_score"] + geo_w * \
                                                   business_similarity["geo_score"]
                    business_similarity = business_similarity.sort_values(by="score", ascending=False)
                business_similarity = business_similarity.head(top_n)
                businesses_similarity = businesses_similarity.append(business_similarity)

        return business_ids, businesses_similarity

    def content_recommend(self, user_id, top_n, filters={}, content_w=0.8, geo_w=0.2):
        logger.info("Starting Content Based Userid...")
        start = time.time()

        docvecs = self.docvecs

        mustArray = [
            self.ye.bodySingleTerm("user_id.keyword", user_id),
            self.ye.bodyRange("date", gteValue="2016-01-01", lteValue="2018-12-31"),
            self.ye.bodyRange("stars", gteValue="3")
        ]
        review_fisrt_chunk = self.ye.getComplexeQuery(index="yelp-review*",
                                                      mustArray=mustArray, filterArray=[],
                                                      exclude=["text", "@timestamp", "@version", "cool", "useful",
                                                               "funny"], size=2000)

        print("Total reviews retrieved: %d" % (review_fisrt_chunk["hits"]["total"]["value"]))
        review_df = self.ye.getResultScrolling(review_fisrt_chunk)
        review_df = review_df.sort_values(by="date", ascending=False)
        # review_df = alreadyreviwed(user_id=user_id, review_df=self.review_df)
        # review_df = review_df[review_df["stars"] >= 3].sort_values(by="date", ascending=False)

        # print("Businesses previously reviewed by user: ")
        # already_reviewed = business_details(self.business_df, review_df)
        # print(already_reviewed[["name", "business_id", "categories"]])

        business_reviewed_ids = review_df["business_id"].unique()
        business_reviewed_ids, sim_business_df = self.business_similarity(business_reviewed_ids, top_n=top_n, filters=filters,
                                                   content_w=content_w, geo_w=geo_w)

        logger.info("Done Content Based Userid in %s seconds." % (time.time() - start))
        return business_reviewed_ids, sim_business_df