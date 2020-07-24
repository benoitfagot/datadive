from elasticquery import ElasticQuery

#Alive Time Scroll
"""
1s = 1 second
1m = 1 minute
"""
scrollTime = "1m"
#Index name
businessIndex = "yelp-business"
reviewIndex = "yelp-review"
tipIndex = "yelp-tip"
checkinIndex = "yelp-checkin"

class YelpQuery(ElasticQuery):

    def __init__(self):
        super().__init__()

    #############################################################
    #                                                           #
    #                   Custom GET methods                      #
    #                                                           #
    #############################################################

    #############################################################
    #                         Business                          #
    #############################################################
    def getAllBusiness(self, size=500):
        res = self.getAllMatch(index=businessIndex, size=size)
        return res

    def getBusinessById(self, businessId, size=500):
        res = self.getSingleMatch(index=businessIndex,
                                field="business_id",
                                value=businessId,
                               size=size)
        return res

    def getBusinessByName(self, businessName, size=500):
        res = self.getSingleMatch(index=businessIndex,
                                field="name",
                                value=businessName,
                               size=size)
        return res

    def getBusinessByState(self, businessState, size=500):
        res = self.getSingleMatch(index=businessIndex,
                                field="state",
                                value=businessState,
                               size=size)
        return res

    def getBusinessByCity(self, businessCity, size=500):
        res = self.getSingleMatch(index=businessIndex,
                                field="city",
                                value=businessCity,
                               size=size)
        return res

    def getBusinessByCategory(self, businessCategory, size=500):
        res = self.getSingleMatch(index=businessIndex,
                                field="categories",
                                value=businessCategory,
                               size=size)
        return res

    #############################################################
    #                          Review                           #
    #############################################################
    def getAllReview(self, size=500):
        res = self.getAllMatch(index=reviewIndex, size=size)
        return res

    def getReviewByReviewId(self, reviewId, exclude=[], size=500):
        res = self.getSingleMatch(index=reviewIndex,
                               field="review_id",
                               value=reviewId,
                               exclude=[],
                               size=size)
        return res

    def getReviewByBusinessId(self, businessId, exclude=[], size=500):
        res = self.getSingleMatch(index=reviewIndex,
                               field="business_id",
                               value=businessId,
                               exclude=[],
                               size=size)
        return res

    def getReviewByStar(self, star, exclude=[], size=500):
        res = self.getSingleMatch(index=reviewIndex,
                               field="stars",
                               value=star,
                               exclude=[],
                               size=size)
        return res
    
    def getReviewByDate(self, term, beginDate=None, endDate=None, size=500):
        res = self.getRangeTerm(index=reviewIndex, 
                                term=term, 
                                beginDate=beginDate, 
                                endDate=endDate, 
                                size=size)
        return res
    
    #############################################################
    #                          Review                           #
    #############################################################
    
    def getAllCheckin(self, size=500):
        res = self.getAllMatch(index=checkinIndex, size=size)
        return res

    def getCheckinByBusinessId(self, businessId, size=500):
        res = self.getSingleMatch(index=checkinIndex,
                               field="business_id",
                               value=businessId,
                               size=size)
        return res