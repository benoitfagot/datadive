import locale
import requests
from django import template
from requests.auth import HTTPBasicAuth
import ast

register = template.Library()


@register.filter(name='multiply')
def multiply(value, arg):
    return value * arg


@register.filter(name='thousand')
def thousand(number):
    locale.setlocale(locale.LC_NUMERIC, 'pl_PL')
    number = format(number, 'n')

    return number


@register.filter(name='get_image')
def get_business_image(img):
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


@register.filter(name='size')
def get_friends_size(friends_list):
    if not friends_list:
        return 0
    else:
        tab = friends_list.split(",")
        return len(tab)


@register.filter(name='get_date')
def get_friends_size(date):
    date = date[0:10]

    return date


@register.filter(name='sort_day')
def sort_day(hours):
    mapping = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    hours = sorted(hours.items(), key=lambda x: mapping[x[0]])

    return hours


@register.filter(name='sort_month')
def sort_month(months):
    mapping = {'January': 0, 'February': 1, 'March': 2, 'April': 3, 'May': 4, 'June': 5, 'July': 6, 'August': 7,
               'September': 8, 'October': 9, 'November': 10, 'December': 11}
    months = sorted(months.items(), key=lambda x: mapping[x[0]])

    return months


@register.filter(name="vf")
def vf(att):
    if att == "True":
        return "Yes"
    if att == "False":
        return "No"
    else:
        return att


@register.filter(name="get_name")
def get_name(user_id):
    ENDPOINT = 'http://47.91.72.40:9200/'
    auth = HTTPBasicAuth('elastic', 'Couronne3...')
    URL = ENDPOINT + 'yelp-user*' + '/_search'

    filter_path = 'hits.hits._source'
    query = 'user_id:"' + user_id + '"'
    field = 'name'

    PARAMS = {'filter_path': filter_path, 'q': query, '_source_includes': field}

    request = requests.get(url=URL, params=PARAMS, auth=auth)

    data = request.json()

    name = data['hits']['hits'][0]['_source']['name']
    if name:
        return name
    else:
        return "Unknown"


@register.filter(name='get_3')
def get_friends_size(day):
    day = day[0:3]

    return day


@register.filter(name='get_value')
def get_value(value):
    if value.startswith('{'):
        return True
    else:
        return False


@register.filter(name='convert_list')
def convert_list(value):
    value_list = list()
    value = ast.literal_eval(value)

    for k, v in value.items():
        if v == False:
            v = "No"
        if v == True:
            v = "Yes"
        new_value = str(k) + ' : ' + str(v)
        value_list.append(new_value)

    return value_list


@register.filter(name='replace_str')
def replace_str(value):
    value = value.replace("u'", "")
    value = value.replace("'", "")

    return value


@register.filter(name='int')
def convert_int(value):
    return int(value)
