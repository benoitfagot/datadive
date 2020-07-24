import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EngineHybrid:
    def __init__(self, cb_rec_model, cf_rec_model, spark_instance, cb_ensemble_weight=1.0, cf_ensemble_weight=1.0):
        logger.info("Initilizing Hybrid Engine")
        self.cb_rec_model = cb_rec_model
        self.cf_rec_model = cf_rec_model
        self.cb_ensemble_weight = cb_ensemble_weight
        self.cf_ensemble_weight = cf_ensemble_weight
        self.spark = spark_instance

    def train(self):
        #self.cb_rec_model.train()
        self.cf_rec_model.train()

    def predict(self, user_id, items_to_ignore=[], topn=20):
        # Getting the top-1000 Content-based filtering recommendations
        cb_recs_df = self.cb_rec_model.predict(user_id=user_id, items_to_ignore=items_to_ignore)\
            .rename(columns={'rec_score': 'rec_score_cb'})

        # Getting the top-1000 Collaborative filtering recommendations
        cf_recs_df = self.cf_rec_model.predict(user_id=user_id, items_to_ignore=items_to_ignore)\
            .rename(columns={'rec_score': 'rec_score_cf'})

        # Combining the results by contentId
        recs_df = cb_recs_df.merge(cf_recs_df,
                                   how='outer',
                                   left_on='business_id',
                                   right_on='business_id').fillna(0.0)

        # Computing a hybrid recommendation score based on CF and CB scores
        recs_df['rec_score_hybrid'] = (recs_df['rec_score_cb'] * self.cb_ensemble_weight) \
                                      + (recs_df['rec_score_cf'] * self.cf_ensemble_weight)

        # Sorting recommendations by hybrid score
        recommendations_df = recs_df.sort_values('rec_score_hybrid', ascending=False).head(topn)
        return recommendations_df
