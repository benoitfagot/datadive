from flask import Flask, Blueprint, request
main = Blueprint('main', __name__)

import logging
from flask import jsonify
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#import findspark
#findspark.init("/home/hongphuc95/anaconda3/lib/python3.7/site-packages/pyspark")
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
#from pyspark.sql.types import StructType, StructField
#from pyspark.sql.types import DoubleType, IntegerType, StringType

import sys
pathModulesES = '/home/hongphuc95/notebookteam/sauceforyall/'
sys.path.append(pathModulesES)
from yelpquery import YelpQuery
from pandasticsearch import Select

from utilcheck import *
import re
from engines_unfinished.enginegeo import EngineGeo
from engines_unfinished.enginepopularity import EnginePopularity
from engines_unfinished.enginecf import EngineCF
from engines_unfinished.enginecb import EngineCB
from engines_unfinished.enginequery import EngineQuery
from engines_unfinished.enginehybrid import EngineHybrid

data_path = "/home/hongphuc95/notebookteam/dataset/"
api_path = "/home/hongphuc95/notebookteam/api/"

##################################################
#                      Fiels                     #
##################################################
def get_fields():
    fields={}

    if request.form.get("topn"):
        fields["topn"] = request.form.get("topn")
    if request.form.get("categories"):
        fields["categories"] = request.form.get("categories")
    if request.form.get("keyword"):
        fields["keyword"] = request.form.get("keyword")
    if request.form.get("userid"):
        fields["userid"] = request.form.get("userid")
    if request.form.get("range"):
        fields["range"] = request.form.get("range")
    if request.form.get("userloc"):
        fields["userloc"] = request.form.get("userloc")

    return fields

##################################################
#                  Geolocation                   #
##################################################
@main.route("/geo/set", methods=["GET", "POST"])
def set_geo():
    userloc = None
    range = 5
    top_n = 20

    filters = {}
    fields = get_fields()

    if "userloc" in fields and fields["userloc"]:
        userloc = fields["userloc"]
    if "range" in fields and fields["range"]:
        range = int(fields["range"])

    if userloc:
        #user_loc["latitude"] = float(36.1672559)
        #user_loc["longitude"] = float(-115.1485163)

        user_input = userloc
        COORDINATE_PATTERN = "^(\-?\d+\.?\d*)\s*\,(\s*\-?\d+\.?\d*)$"
        if re.match(COORDINATE_PATTERN, user_input):
            match_res = re.search(COORDINATE_PATTERN, user_input)
            user_loc = {}
            user_loc["latitude"] = float(match_res.group(1))
            user_loc["longitude"] = float(match_res.group(2))
            engine_geo.recommend(lookup=user_loc, engine=False, rec_range=range)
            coor_res = engine_geo.get_current_location()
            print("Done retriving businesses informations in the area by coordinates")
            return jsonify(coor_res)
        else:
            engine_geo.recommend(lookup=user_input, engine=True, rec_range=range)
            coor_res = engine_geo.get_current_location()
            print("Done retriving businesses informations in the area by users address")
            return jsonify(coor_res)
    else:
        engine_geo.reset()
        coor_res = engine_geo.get_current_location()
        print("Nothing entered. Use locations around US")
        return jsonify(coor_res)

@main.route("/geo/location", methods=["GET"])
def get_geo():
    coor_res = engine_geo.get_current_location()
    print("Getting geolocation...")
    return jsonify(coor_res)


##################################################
#                Popularity Based                #
##################################################
@main.route("/pop/recommend", methods=["GET", "POST"])
def recommend_pop():
    top_n = 20

    if request.method == "POST":
        filters = {}
        fields = get_fields()

        if "topn" in fields:
            top_n = int(fields["topn"])
        if "categories" in fields:
            filters["categories"] = fields["categories"]

        nearby_df = engine_geo.get_business_nearby()
        if not nearby_df.empty:
            filters["nearby"] = nearby_df
        print(filters)

        logger.debug("Launching Popularity Recomendation")
        recommendations_df = engine_pop.recommend(top_n=top_n, filters=filters, elastic=False)
        output = {"rated": [], "recommended": [recommendations_df.to_dict(orient='records')]}
        return jsonify(output)

##################################################
#                 Content Based                  #
##################################################

@main.route("/cb/load", methods=["GET"])
def load_cb():
    logger.debug("Loading CB Engine")
    engine_cb.load()
    return "CB Engine Loaded"

@main.route("/cb/tovec", methods=["GET"])
def tovec_cb():
    logger.debug("Conversion User Reviews to Vectors CB Engine")
    engine_cb.train_texttovec()
    return "Done Conversion"

@main.route("/cb/recommend", methods=["GET", "POST"])
def recommend_cb():
    user_id = None
    keyword = None

    top_n = 20

    if request.method == "POST":
        filters = {}
        fields = get_fields()

        if "topn" in fields and fields["topn"]:
            top_n = int(fields["topn"])
        if "keyword" in fields and fields["keyword"]:
            keyword = fields["keyword"]
        if "userid" in fields and fields["userid"]:
            user_id = fields["userid"]

        nearby_df = engine_geo.get_business_nearby()
        if not nearby_df.empty:
            filters["nearby"] = nearby_df
        if "categories" in fields and fields["categories"]:
            filters["categories"] = fields["categories"]
        print(filters)

        if keyword:
            logger.debug("Launching CB recommender for keyword %s", keyword)
            recommendations_df = engine_cb.keyword_recommend(input_str=keyword, top_n=top_n, filters=filters)
            output = {"rated": [], "recommended": [recommendations_df.to_dict(orient='records')]}
            return jsonify(output)

        if user_id:
            logger.debug("Launching CB recommender for userid %s", user_id)
            already_reviewed, recommendations_df = engine_cb.content_recommend(user_id=user_id, top_n=top_n, filters=filters)
            output = {"rated": already_reviewed.tolist(), "recommended": [recommendations_df.to_dict(orient='records')]}
            return jsonify(output)


##################################################
#            Collaborative Filtering             #
##################################################

@main.route("/cf/train", methods=["GET"])
def train_cf():
    logger.debug("Building CF model")
    engine_cf.train(n_factor=20)
    return "Training CF complete"

@main.route("/cf/load", methods=["GET"])
def load_cf():
    logger.debug("Loading Custom SVD CF model")
    engine_cf.load()
    return "CF model loaded"

@main.route("/cf/save", methods=["GET"])
def save_cf():
    logger.debug("Savind Custom SVD CF model")
    engine_cf.save_model()
    return "CF model saved"

@main.route("/cf/recommend", methods=["GET", "POST"])
def recommend_cf():
    user_id = None
    top_n = 20

    if request.method == "POST":
        filters = {}
        fields = get_fields()

        if "topn" in fields:
            top_n = int(fields["topn"])
        if "userid" in fields:
            user_id = fields["userid"]

        nearby_df = engine_geo.get_business_nearby()
        if not nearby_df.empty:
            filters["nearby"] = nearby_df
        print(filters)

        logger.debug("Lauching CF recommender for user %s", user_id)
        already_reviewed, recommendations_df = engine_cf.predict(user_id=user_id, topn=top_n, filters=filters)
        #if already_reviewed.size == 0:
        #    already_reviewed = []
        #else:
        #    already_reviewed = already_reviewed.tolist()
        output = {"rated": [already_reviewed.to_dict(orient='records')], "recommended": [recommendations_df.to_dict(orient='records')]}
        #output = {"recommended": [recommendations_df.to_dict(orient='records')]}
        return jsonify(output)

@main.route("/cf/similar", methods=["GET", "POST"])
def similar_cf():
    topn = 5
    user_being_recommneded = ""
    user_similar = ""

    if request.method == "POST":
        if request.form.get("user_being_recommneded"):
            user_being_recommneded = request.form.get("user_being_recommneded")
        if request.form.get("user_similar"):
            user_similar = request.form.get("user_similar")

        if user_being_recommneded and user_similar:
            business_in_common_df = engine_cf.business_in_common(user_x=user_being_recommneded, user_y=user_similar, topn=topn)
            output = {"results": business_in_common_df.to_dict(orient="records")}
            return output


@main.route("/datareload", methods=["GET"])
def data_reload():
    logger.debug("Reload data dataframe")
    #inti_data()
    return "Reloading complete"

@main.route("/info/business/<businessid>", methods=["GET"])
def get_info_business(businessid):
    logger.debug("Get info business %s" % (businessid))
    business_df = engine_query.get_business_info(business_id=businessid)
    business_df = business_df.to_dict(orient='records')
    return jsonify(business_df)

# @main.route("/build/hybrid", methods=["GET"])
# def build_hybrid():
#     logger.debug("Building Hybrid model")
#     engine_hybrid.train()
#     return "Training Hybrid complete"
#

# @main.route("/recommend/hybrid/<string:user_id>/<int:top_n>", methods=["GET"])
# def recommend_hybrid(user_id, top_n):
#     logger.debug("Lauching Hybrid recommender for user %s", user_id)
#     already_reviewed = engine_cb.already_review(user_id)
#     recommendations_df = engine_hybrid.predict(user_id=user_id, topn=top_n, items_to_ignore=already_reviewed)
#     output = {"rated": already_reviewed.tolist(), "recommended": [recommendations_df.to_dict(orient='records')]}
#     return jsonify(output)
#
# @main.route("/rated/cf/<string:user_id>", methods=["GET"])
# def already_rated_cf(user_id):
#     logger.debug("Show rated businesses by user %s", user_id)
#     already_reviewed = engine_cf.already_review(user_id)
#     output = {"rated": already_reviewed.tolist()}
#     return jsonify(output)
#
# @main.route("/rated/cb/<string:user_id>", methods=["GET"])
# def already_rated_cb(user_id):
#     logger.debug("Show rated businesses by user %s", user_id)
#     already_reviewed = engine_cb.already_review(user_id)
#     output = {"rated": already_reviewed.tolist()}
#     return jsonify(output)
#
# @main.route("/rated/hybrid/<string:user_id>", methods=["GET"])
# def already_rated_hybrid(user_id):
#     logger.debug("Show rated businesses by user %s", user_id)
#     already_reviewed = engine_hybrid.already_review(user_id)
#     output = {"rated": already_reviewed.tolist()}
#     return jsonify(output)

def create_app():
    global fields
    global engine_geo
    global engine_pop
    global engine_cf
    global engine_cb
    global engine_hybrid
    global engine_query
    ye = YelpQuery()

    #spark = SparkSession.builder \
    #    .appName('yelp_server') \
    #    .master("local[*]") \
    #    .getOrCreate()

    #business_df, review_df = inti_data()

    logger.debug("Start loading data")
    review_df = pd.read_csv("/home/hongphuc95/notebookteam/fu/review.csv")

    checkin_df = pd.read_json(data_path + "/trends/checkin_per_business_elite.json", lines=True)
    business_df = pd.read_json(data_path + "business.json", lines=True)
    business_df = business_df.dropna(subset=["categories"]).reset_index(drop=True)
    logger.debug("Done loading data")


    engine_query = EngineQuery(ye=ye)
    engine_geo = EngineGeo(business_df=business_df)
    engine_cf = EngineCF(review_df=review_df, business_df=business_df, ye=ye)
    engine_pop = EnginePopularity(business_df=business_df, ye=ye, review_df=review_df, checkin_df=checkin_df)
    engine_cb = EngineCB(ye=ye, business_df=business_df)


    #engine_hybrid = EngineHybrid(cb_rec_model=engine_cb, cf_rec_model=engine_cf, spark_instance=spark)

    engine_cf.train()
    engine_cb.load()

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
