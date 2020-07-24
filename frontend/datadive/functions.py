import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import folium
from datetime import datetime
import json
from geopy.geocoders import Nominatim

ENDPOINT = 'http://47.91.72.40:9200/'
auth = HTTPBasicAuth('elastic', 'Couronne3...')
BUSINESS_INDEX = 'yelp-cleaned-business*'
USER_INDEX = 'yelp-user*'
AFFLUENCE_INDEX = 'yelp-affluence*'
REVIEW_INDEX = 'yelp-review*'

format_date = '%Y-%m-%d %H:%M:%S'
now = datetime.now().strftime(format_date)


def home_businesses(city="Las Vegas"):
    '''Function to retrieve a list of business to show to the user'''
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    size = 10
    query = 'city:"' + city + '"'
    sort = 'review_count:desc'
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size, 'sort': sort}
    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    return data['hits']['hits']


def trends_recommandation(categorie="Food, Restaurant"):
    URL = ENDPOINT + 'yelp-season-moment*' + '/_search'
    business_list = list()

    filter_path = 'hits.hits._source'
    query = 'season:"' + get_season(now) + '" AND moment:"' + get_moment(now) \
            + '" AND categories_label:"' + categorie + '"'
    field = 'business_id'
    PARAMS = {'filter_path': filter_path, 'q': query, '_source_includes': field}
    request = requests.get(url=URL, params=PARAMS, auth=auth)
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'
    data = request.json()

    for x in range(len(data['hits']['hits'])):
        business_list.append(data['hits']['hits'][x]['_source']['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    filter_path = 'hits.hits._source'
    query = 'business_id:' + business_list
    sort = 'review_count:asc'
    PARAMS = {'filter_path': filter_path, 'q': query, 'sort': sort}
    request = requests.get(url=URL, params=PARAMS, auth=auth)
    data = request.json()

    columns_name = list(data['hits']['hits'][0]['_source'].keys())
    df_business = pd.DataFrame(data['hits']['hits'])

    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    final = df_business.to_dict('records')

    return final


def trends_recommandation_city(categorie="Food, Restaurant", city="Las Vegas"):
    URL = ENDPOINT + 'yelp-season-moment*' + '/_search'
    business_list = list()

    filter_path = 'hits.hits._source'
    query = 'season:"' + get_season(now) + '" AND moment:"' + get_moment(now) \
            + '" AND categories_label:"' + categorie + '" AND city:"' + city + '"'
    field = 'business_id'
    PARAMS = {'filter_path': filter_path, 'q': query, '_source_includes': field}
    request = requests.get(url=URL, params=PARAMS, auth=auth)
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'
    data = request.json()

    for x in range(len(data['hits']['hits'])):
        business_list.append(data['hits']['hits'][x]['_source']['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    filter_path = 'hits.hits._source'
    query = 'business_id:' + business_list
    sort = 'review_count:asc'
    PARAMS = {'filter_path': filter_path, 'q': query, 'sort': sort}
    request = requests.get(url=URL, params=PARAMS, auth=auth)
    data = request.json()

    columns_name = list(data['hits']['hits'][0]['_source'].keys())
    df_business = pd.DataFrame(data['hits']['hits'])

    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    final = df_business.to_dict('records')

    return final


def get_businesses(city, categorie='Food, Restaurant', size=6):
    '''Function to retrieve a list of business to show to the user'''
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'city:"' + city + '" AND ' + 'categories_label:"' + categorie + '"'
    size = size
    sort = 'review_count:desc'
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size, 'sort': sort}

    request = requests.get(url=URL, params=PARAMS, auth=auth)
    data = request.json()

    return data['hits']['hits']


def get_businesses_by_categorie(city="Las Vegas", categorie="Food, Restaurant", size=10):
    '''Function to retrieve a list of business to show to the user'''
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'city:"' + city + '" AND ' + 'categories_label:"' + categorie + '"'
    size = size
    sort = 'review_count:desc'
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size, 'sort': sort}

    request = requests.get(url=URL, params=PARAMS, auth=auth)
    data = request.json()

    return data['hits']['hits']


def get_business_image(img):
    '''Function to retrieve business image from YELP API'''
    URL = "https://api.yelp.com/v3/businesses/" + img
    headers = {
        "Authorization": "Bearer XxMwpmqG1ZCITKpSg35VNpTFGHOXyFcttiQwC87xV1wLcVdm1-jeJRztnOX4q9LAMxZUaWovmYtWJeFGs5zWUqRYjHskMkaI-3IGXvjhq66oHGlQPhmsFmkvNnrJXnYx"}

    request = requests.get(url=URL, headers=headers).json()

    request = dict(request)
    first = list(request.keys())[0]
    if first == "id":
        return request['image_url']
    else:
        return "https://image.freepik.com/vecteurs-libre/restaurant-logo-modele_1236-155.jpg"


def get_users_id(size=25):
    URL = ENDPOINT + USER_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    size = size
    field = 'user_id'
    sort = 'fans:desc'
    PARAMS = {'filter_path': filter_path, '_source_includes': field, 'sort': sort, 'size': size}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    return data['hits']['hits']


def get_user_detail(user_id="Vide"):
    URL = ENDPOINT + USER_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'user_id:"' + user_id + '"'

    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    return data['hits']['hits']


def get_business_by_id(business_id):
    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'business_id:"' + business_id + '"'
    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    return data['hits']['hits'][0]['_source']


def get_affluence(business_id):
    URL = ENDPOINT + AFFLUENCE_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'business_id:"' + business_id + '"'

    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    if len(data) != 0:
        return data['hits']['hits']
    else:
        return data


def get_reviews(business_id):
    URL = ENDPOINT + REVIEW_INDEX + '/_search'

    filter_path = 'hits.hits._source'
    query = 'business_id:"' + business_id + '"'

    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    return data['hits']['hits']


def set_user_location(location):
    url = "http://34.76.62.206:5000/geo/set"
    PARAMS = {'userloc': location}

    request = requests.post(url=url, data=PARAMS)

    data = request.json()


def get_user_city(user_id):
    URL = ENDPOINT + 'yelp-city-user*' + '/_search'

    filter_path = 'hits.hits._source'
    query = 'user_id:' + user_id

    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    return data['hits']['hits'][0]['_source']['user_city']


def get_keyword_recommandation(keyword, size=10):
    url = "http://34.76.62.206:5000/cb/recommend"
    PARAMS = {'keyword': keyword, 'topn': size}

    request = requests.post(url=url, data=PARAMS)

    data = request.json()
    results = data['recommended'][0]
    business_list = list()
    for x in range(len(results)):
        business_list.append(results[x]['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'

    query = 'business_id:' + business_list
    size = len(results)
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    businesses = data['hits']['hits']

    columns_name = list(businesses[0]['_source'].keys())
    df_business = pd.DataFrame(businesses)
    df_results = pd.DataFrame(results)
    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    df = pd.merge(df_business, df_results, on='business_id')
    final = df.to_dict('records')

    return final


# Function for known user's home page
def get_cb_recommandation(user_id, size=2):
    url = "http://34.76.62.206:5000/cb/recommend"
    PARAMS = {'userid': user_id, 'topn': size}

    request = requests.post(url=url, data=PARAMS)

    data = request.json()
    results = data['recommended'][0]
    business_list = list()
    for x in range(len(results)):
        business_list.append(results[x]['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'

    query = 'business_id:' + business_list
    size = len(results)
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    businesses = data['hits']['hits']

    columns_name = list(businesses[0]['_source'].keys())
    df_business = pd.DataFrame(businesses)
    df_results = pd.DataFrame(results)
    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    df = pd.merge(df_business, df_results, on='business_id')

    df[['business_id', 'input_business_id', 'content_score', 'score']].to_csv('test.csv', index=False)

    final = df.to_dict('records')

    return final


def get_cb_categories_recommandation(categorie, user_id, size=2):
    url = "http://34.76.62.206:5000/cb/recommend"
    PARAMS = {'categories': categorie, 'userid': user_id, 'topn': size}

    request = requests.post(url=url, data=PARAMS)

    data = request.json()
    results = data['recommended'][0]
    business_list = list()
    for x in range(len(results)):
        business_list.append(results[x]['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'

    query = 'business_id:' + business_list
    size = len(results)
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    businesses = data['hits']['hits']

    columns_name = list(businesses[0]['_source'].keys())
    df_business = pd.DataFrame(businesses)
    df_results = pd.DataFrame(results)
    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    df = pd.merge(df_business, df_results, on='business_id')

    df[['business_id', 'input_business_id', 'content_score', 'score']].to_csv('test.csv', index=False)

    final = df.to_dict('records')

    return final


def get_pop_recommandation(categorie="Restaurants", size=10):
    url = "http://34.76.62.206:5000/pop/recommend"
    PARAMS = {'categories': categorie, 'topn': size}

    request = requests.post(url=url, data=PARAMS)

    data = request.json()
    results = data['recommended'][0]
    business_list = list()
    for x in range(len(results)):
        business_list.append(results[x]['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'

    query = 'business_id:' + business_list
    size = len(results)
    PARAMS = {'filter_path': filter_path, 'q': query, 'size': size}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    businesses = data['hits']['hits']

    columns_name = list(businesses[0]['_source'].keys())

    df_business = pd.DataFrame(businesses)
    df_results = pd.DataFrame(results)

    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    df = pd.merge(df_business, df_results, on='business_id')
    final = df.to_dict('records')

    return final


def get_cf_recommandation(user_id, size=10):  # SET DEFAULT VALUE FOR CATEGORIE
    url = "http://34.76.62.206:5000/cf/recommend"
    PARAMS = {'userid': user_id, 'topn': size}

    request = requests.post(url=url, data=PARAMS)
    data = request.json()
    results = data['recommended'][0]
    business_list = list()
    for x in range(len(results)):
        business_list.append(results[x]['business_id'])

    business_list = json.dumps(business_list)
    business_list = business_list.replace("[", "")
    business_list = business_list.replace("]", "")
    business_list = business_list.replace(",", " OR")

    URL = ENDPOINT + BUSINESS_INDEX + '/_search'

    filter_path = 'hits.hits._source'

    query = 'business_id:' + business_list

    PARAMS = {'filter_path': filter_path, 'q': query}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()
    businesses = data['hits']['hits']

    columns_name = list(businesses[0]['_source'].keys())

    df_business = pd.DataFrame(businesses)
    df_results = pd.DataFrame(results)

    for col in columns_name:
        df_business[col] = df_business['_source'].apply(lambda x: x[col])

    df_business = df_business.drop('_source', axis=1)
    df = pd.merge(df_business, df_results, on='business_id')
    final = df.to_dict('records')

    return final


def get_map_with_city(geolocation, default_businesses=[], cb_recommandations=[], cf_recommandations=[]):
    m = folium.Map([geolocation[0], geolocation[1]], zoom_start=14)
    folium.Marker([geolocation[0], geolocation[1]], popup="Your position", icon=folium.Icon(color='purple')).add_to(m)

    if len(default_businesses) > 0:
        for x in default_businesses:
            lt = x['latitude']
            lg = x['longitude']
            folium.Marker([lt, lg], popup=x['categories'], icon=folium.Icon(color='red')).add_to(m)

    if len(default_businesses) > 0:
        for x in cb_recommandations:
            lt = x['latitude']
            lg = x['longitude']
            folium.Marker([lt, lg], popup=x['categories'], icon=folium.Icon(color='blue')).add_to(m)

    if len(default_businesses) > 0:
        for x in cf_recommandations:
            lt = x['latitude']
            lg = x['longitude']
            folium.Marker([lt, lg], popup=x['categories'], icon=folium.Icon(color='green')).add_to(m)

    folium.Marker([49.049603, 2.0774914], popup="&#x1F424", icon=folium.Icon(color='green')).add_to(m)
    m = m._repr_html_().replace("height:0", "height:100%")
    m = m.replace('style="width:100%;"', 'style="width:100%;height:100%"')

    return m


def get_map_without_city(default_businesses, recommandations):
    m = folium.Map([default_businesses[0]['latitude'], default_businesses[0]['longitude']],
                   zoom_start=14)

    for x in default_businesses:
        lt = x['latitude']
        lg = x['longitude']
        folium.Marker([lt, lg], popup=x['name'], icon=folium.Icon(color='red')).add_to(m)

    for x in recommandations:
        lt = x['latitude']
        lg = x['longitude']
        folium.Marker([lt, lg], popup=x['name'], icon=folium.Icon(color='blue')).add_to(m)

    folium.Marker([49.049603, 2.0774914], popup="&#x1F424", icon=folium.Icon(color='green')).add_to(m)
    m = m._repr_html_().replace("height:0", "height:100%")
    m = m.replace('style="width:100%;"', 'style="width:100%;height:100%"')

    return m


def get_season(date):
    date = datetime.strptime(date, format_date)

    month = date.month
    day = date.day

    # Spring
    if month in (3, 4, 5, 6):
        if month in (3, 6):
            if (month == 3) & (day >= 19) | (month == 6) & (day < 20):
                season = 'Spring'
        else:
            season = 'Spring'

    # Summer
    if month in (6, 7, 8, 9):
        if month in (6, 9):
            if (month == 6) & (day >= 20) | (month == 9) & (day < 22):
                season = 'Summer'
        else:
            season = 'Summer'

    # Autumn
    if month in (9, 10, 11, 12):
        if month in (9, 12):
            if (month == 9) & (day >= 22) | (month == 12) & (day < 21):
                season = 'Autumn'
        else:
            season = 'Autumn'

    # Winter
    if month in (12, 1, 2, 3):
        if month in (12, 3):
            if ((month == 12) & (day >= 21)) | ((month == 3) & (day < 19)):
                season = 'Winter'
        else:
            season = 'Winter'

    return season;

    def get_season(date):
        date = datetime.strptime(date, format_date)

    month = date.month
    day = date.day

    # Spring
    if month in (3, 4, 5, 6):
        if month in (3, 6):
            if (month == 3) & (day >= 19) | (month == 6) & (day < 20):
                season = 'Spring'
        else:
            season = 'Spring'

    # Summer
    if month in (6, 7, 8, 9):
        if month in (6, 9):
            if (month == 6) & (day >= 20) | (month == 9) & (day < 22):
                season = 'Summer'
        else:
            season = 'Summer'

    # Autumn
    if month in (9, 10, 11, 12):
        if month in (9, 12):
            if (month == 9) & (day >= 22) | (month == 12) & (day < 21):
                season = 'Autumn'
        else:
            season = 'Autumn'

    # Winter
    if month in (12, 1, 2, 3):
        if month in (12, 3):
            if ((month == 12) & (day >= 21)) | ((month == 3) & (day < 19)):
                season = 'Winter'
        else:
            season = 'Winter'

    return season;


def get_moment(date):
    hour = datetime.strptime(date, format_date).hour

    if hour in range(6, 12):
        moment = 'Morning'
    if hour in range(12, 17):
        moment = 'Afternoon'
    if hour in range(17, 22):
        moment = 'Evening'
    if hour in range(22, 24) or hour in range(0, 6):
        moment = 'Night'

    return moment;


def geolocation():
    token = "5e599797dbecfc222d30063da4b86640"
    send_url = "http://api.ipstack.com/check?access_key=" + token

    result = []

    geo_req = requests.get(send_url)
    response = geo_req.json()
    result.append(float(response['latitude']))
    result.append(float(response['longitude']))
    result.append(response['city'])
    result.append(response['country_name'])

    return result[2]


def get_coordinates(city):
    coordinates = []
    geolocator = Nominatim(user_agent="Data_Dive")
    location = geolocator.geocode(city)

    coordinates.append(location.latitude)
    coordinates.append(location.longitude)

    return coordinates


def get_users_list():
    users_list = ['bLbSNkLggFnqwNNzzq-Ijw', 'PcvbBOCOcs6_suRDH7TSTg', 'I-4KVZ9lqHhk8469X9FvhA',
                  '6Ki3bAL0wx9ymbdJqbSWMA',
                  'ic-tyi1jElL_umxZVh8KNA', 'CxDOIDnH8gp9KXzpBHJYXw', 'qKpkRCPk4ycbllTfFcRbNw',
                  'ELcQDlf69kb-ihJfxZyL0A',
                  'DK57YibC5ShBmqQl97CKog', '3nIuSCZk5f_2WWYMLN7h3w', 'xDl9ZF3SckkZde_48W6WeA',
                  '5CgjjDAic2-FAvCtiHpytA',
                  'U4INQZOPSUaj8hMjLlZ3KA', 'JaqcCU3nxReTW2cBLHounA', '9gZ4pQHdK6v8xMLig6EEFA',
                  'YBT3EKUNN4IP8m4x7sGu1g',
                  'u3ZPMVVEzneq8x856WksJQ', 'JLv2Dmfj73-I0d9N41tz1A', 'keBv05MsMFBd0Hu98vXThQ',
                  'PGeiszoVusiv0wTHVdWklA',
                  'D43OWyfzIQjL8feJpYh2SQ', 'MMf0LhEk5tGa1LvN7zcDnA', 'U1vl4SQzO3wTAWlYVnSjnw',
                  'nyl_1VcRIAyI55bb_scpdw',
                  'cMEtAiW60I5wE_vLfTxoJQ']

    return users_list


def read_input(business_id):
    df = pd.read_csv('test.csv')
    input = df[df['business_id'] == business_id]['input_business_id'].iloc[0]
    content_score = df[df['business_id'] == business_id]['content_score'].iloc[0]
    score = df[df['business_id'] == business_id]['score'].iloc[0]
    print(input)
    return input, content_score, score
