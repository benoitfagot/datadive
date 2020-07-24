from elasticsearch import Elasticsearch
from pandasticsearch import Select

#Alive Time Scroll
"""
1s = 1 second
1m = 1 minute
"""
scrollTime = "1m"

# Credentials
connCredential = {}
connCredential['host'] = "http://47.91.72.40"
connCredential['port'] = "9200"
connCredential['conn'] = [connCredential['host'] + ":" + connCredential['port']]

class ElasticQuery:
    es = Elasticsearch(connCredential['conn'], http_auth=('elastic', 'Couronne3...'))

    def __init__(self):
        pass

    def getInstaceES(self):
        return self.es

    #############################################################
    #                                                           #
    #                      GENERATE BODY                        #
    #                                                           #
    #############################################################

    #############################################################
    #                           QUERY                           #
    #############################################################
    def singleQuery(self, operation):
        query = {
            "query": operation
        }
        return query

    def multiQuery(self, mustArray, filterArray):
        query = {
          "query": {
            "bool": {
              "must": mustArray,
              "filter": filterArray
            }
          }
        }
        return query

    #############################################################
    #                           MATCH                           #
    #############################################################
    def bodySingleMatch(self, field, value):
        """
            "query": {
                "match" : {
                    "message" : "this is a test"
                }
            }
        """

        body = { "match": { field : value} }
        return body


    def bodyMatchAll(self):
        body = { "match_all": {} }
        return body

    #############################################################
    #                          TERM                             #
    #############################################################
    def bodySingleTerm(self, term, termValue):
        body = {
            "term": {
                term: termValue
            }
        }
        return body

    def bodyMultivalueTerm(self, term, termValueArray):
        body = {
            "terms": {
                term: termValueArray
            }
        }
        return body
    
    def bodyRange(self, term, gteValue = None, lteValue = None):
        arrayRange = {}
        if gteValue:
            arrayRange["gte"] = gteValue
        if lteValue:
            arrayRange["lte"] = lteValue

        body = {
            "range": {
              term: arrayRange
            }
        }
        return body
    
    def bodyUpdate(self, scriptedField):
        body = {
            "script": scriptedField
        }
        return body
    
    def bodyUpdateByQuery(self, scriptedField, matchArray):
        body = {
            "script": scriptedField,
            "query": {
                "term": matchArray
            }
        }
        return body

    #############################################################
    #                                                           #
    #                        GET METHODS                        #
    #                                                           #
    #############################################################
    def getAllMatch(self, index, exclude=[], include=[], size=500):
        allMatch = self.bodyMatchAll()
        body = self.singleQuery(allMatch)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result

    def getSingleMatch(self, index, field, value, exclude=[], include=[], size=500):
        singleMatch = self.bodySingleMatch(field, value)
        body = self.singleQuery(singleMatch)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result

    def getSingleTerm(self, index, term, value, exclude=[], include=[], size=500):
        singleTerm = self.bodySingleTerm(term, value)
        body = self.singleQuery(singleTerm)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result

    def getMultivalueTerm(self, index, term, valueArray, exclude=[], include=[], size=500):
        multivalueTerm = self.bodyMultivalueTerm(term, valueArray)
        body = self.singleQuery(multivalueTerm)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result
    
    def getRangeTerm(self, index, term, beginDate=None, endDate=None, exclude=[], include=[], size=500):
        rangeTerm = self.bodyRange(term, beginDate, endDate)
        body = self.singleQuery(rangeTerm)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result

    def getComplexeQuery(self, index, mustArray, filterArray, exclude=[], include=[], size=500):
        body = self.multiQuery(mustArray, filterArray)

        result = self.es.search(
            body=body,
            index=index,
            size=size,
            _source_excludes=exclude,
            _source_includes=include,
            scroll=scrollTime
        )
        return result
    
    def getResultScrolling(self, partRes, partialSize=None):
        df = None
        scrollId = partRes['_scroll_id']

        # Do you want the whole index or a partial data ?
        scrollSize = partRes['hits']['total']['value']
        if partialSize:
            if partialSize <= scrollSize:
                scrollSize = partialSize

        while scrollSize > 0:
            #print(scrollSize)

            pandas_df = Select.from_dict(partRes).to_pandas()
            chunkSize = len(partRes['hits']['hits'])
            if df is None:
                df = pandas_df
            else:
                df = df.append(pandas_df, ignore_index=True)
            # arrayHits = partRes['hits']['hits']
            # for hit in arrayHits:
            #    for key, val in hit["_source"].items():
            #        try:
            #            fields[key] = np.append(fields[key], val)
            #        except KeyError:
            #            fields[key] = np.array([val])          

            scrollSize = scrollSize - chunkSize
            partRes = self.scroll(scroll_id=scrollId)
           

        df.drop(columns=["_index", "_type", "_id", "_score"] , axis=1, inplace=True)
        return df

    def scroll(self, scroll_id):
        result = self.es.scroll(scroll_id=scroll_id, scroll=scrollTime)
        return result

    #############################################################
    #                                                           #
    #                          UPDATE                           #
    #                                                           #
    #############################################################

    def updateDoc(self, index, docID, scriptedField, params=None):
        body = self.bodyUpdate(scriptedField=scriptedField)

        # just do it
        if not params:
            self.es.update(index=index, id=docID, body=body)
        else:
            self.es.update(index=index, id=docID, body=body, params=params)


    def updateQueryDoc(self, index, matchArray, scriptedField, params=None):
        body = self.bodyUpdateByQuery(scriptedField=scriptedField, matchArray=matchArray)

        # just do it
        if not params:
            self.es.update(index=index, body=body)
        else:
            self.es.update(index=index, body=body, params=params)


    #Examples
    """
    if __name__ == '__main__':
    print("Hello World")
    #res = getAllMatch(index="yelp-business*", size=1000)
    #res = getSingleMatch(index="yelp-business*", field="business_id", value="eTL9C2RgAHU0fGoLbRvqbw", size=1000)
    #res = getSingleTerm(index="yelp-business*", term="business_id.keyword", value="eTL9C2RgAHU0fGoLbRvqbw", size=1000)
    #res = getSingleMatch(index="yelp-business*", field="state", value="CA", size=1000)
    #res = getMultivalueTerm(index="yelp-business*", term="business_id.keyword", valueArray=["eTL9C2RgAHU0fGoLbRvqbw", "5CjdVHY504ccwZ2n3uyPOA"], size=1000)

    mustArray = [
        bodySingleTerm("state.keyword", "AZ"),
        bodyMultivalueTerm("city.keyword", ["Phoenix", "Tempe"]),
    ]
    filterArray = []
    res = getComplexeQuery(index="yelp-business*", mustArray=mustArray, filterArray=filterArray, size=1000)
    print(res)
    """