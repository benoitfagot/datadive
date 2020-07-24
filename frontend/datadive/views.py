from django.shortcuts import render
from django.shortcuts import redirect
from . import functions as f
import random
import pandas as pd


# Create your views here.
def home(request):
    if 'city' in request.session:
        print(request.session['city'])
    if not 'user_id' in request.session or request.session['user_id'] == 'new_user':
        print("Cas new user")
        if 'city' in request.session:
            default_businesses = f.get_pop_recommandation()
            cb_user_id = {}
            cf_user_id = {}
            m = f.get_map_with_city(f.get_coordinates(request.session['city']), default_businesses, cb_user_id,
                                    cf_user_id)
            print("Geolocated cb_user_id")
        else:
            default_businesses = f.trends_recommandation()
            cb_user_id = {}
            cf_user_id = {}
            m = f.get_map_without_city(default_businesses, cb_user_id)
            print("Trends cb_user_id")
    else:
        cb_user_id = f.get_cb_recommandation(request.session['user_id'])
        default_businesses = f.get_pop_recommandation()
        cf_user_id = f.get_cf_recommandation(request.session['user_id'])
        m = f.get_map_with_city(f.get_coordinates(request.session['city']), default_businesses, cb_user_id, cf_user_id)

    return render(request, 'index.html',
                  {'cb_user_id': cb_user_id, 'default_businesses': default_businesses, 'my_map': m,
                   'cf_user_id': cf_user_id})


def geolocation(request):
    request.session['city'] = f.geolocation()
    f.set_user_location(request.session['city'])
    print('Geolocated at ' + request.session['city'])

    return redirect('/home')


def search(request):
    keyword = request.POST.get("keyword")

    if 'city' in request.session:
        city = request.session['city']

    if not 'user_id' in request.session or request.session['user_id'] == 'new_user':
        print("New user")
        if 'city' in request.session:
            default_businesses = f.get_keyword_recommandation(keyword)
            cb_user_id = []
            cf_user_id = []
            m = f.get_map_with_city(f.get_coordinates(city), default_businesses)
        else:
            default_businesses = f.trends_recommandation()
            cb_user_id = []
            cf_user_id = []
            m = f.get_map_with_city(f.get_coordinates(city), default_businesses)
    else:
        print("Known user")
        default_businesses = f.get_keyword_recommandation(keyword)
        cb_user_id = f.get_cb_recommandation(request.session['user_id'])
        cf_user_id = f.get_cf_recommandation(request.session['user_id'])
        m = f.get_map_with_city(f.get_coordinates(city), default_businesses, cb_user_id, cf_user_id)

    return render(request, 'index.html',
                  {'cb_user_id': cb_user_id, 'default_businesses': default_businesses, 'cf_user_id': cf_user_id,
                   'my_map': m})


def categorie(request, categorie):
    if not 'user_id' in request.session or request.session['user_id'] == 'new_user':
        if 'city' in request.session:
            city = request.session['city']
            default_businesses = f.trends_recommandation_city(categorie, city)
            cb_categories_user_id = []
            cf_user_id = []
            m = f.get_map_with_city(
                f.get_coordinates(request.session['city']), default_businesses, cb_categories_user_id, cf_user_id)
        else:
            default_businesses = f.trends_recommandation(categorie)
            cb_categories_user_id = []
            cf_user_id = []
            m = f.get_map_without_city(default_businesses, cb_categories_user_id)
    else:
        default_businesses = f.get_pop_recommandation(categorie, 10)
        cb_categories_user_id = f.get_cb_categories_recommandation(categorie, request.session['user_id'])
        cf_user_id = f.get_cf_recommandation(request.session['user_id'])
        m = f.get_map_with_city(
            f.get_coordinates(request.session['city']), default_businesses, cb_categories_user_id, cf_user_id)

    return render(request, 'pages/categorie.html',
                  {'default_businesses': default_businesses, 'cb_categories_user_id': cb_categories_user_id,
                   'cf_user_id': cf_user_id, 'my_map': m})


def profile(request):
    user_info = {}
    city = ""

    if 'user_id' in request.POST:
        request.session['user_id'] = request.POST['user_id']
        user_id = request.POST.get("user_id")
        if user_id != "new_user":
            user_info = f.get_user_detail(user_id)
            city = f.get_user_city(user_id)
            request.session['city'] = city
        else:
            user_info = [{'business_id': 'new_user'}]
            city = "nowhere"

    if 'address' in request.POST and request.POST.get("address") != "":
        request.session['city'] = request.POST.get("address")

    if 'city' in request.session:
        f.set_user_location(request.session['city'])

    users = f.get_users_list()
    return render(request, 'pages/profile.html',
                  {'users': users, 'info': user_info, 'city': city})


def business(request, business_id):
    df = pd.read_csv('test.csv')
    if len(df[df['business_id'] == business_id]) != 0:
        input_business_id, content_score, score = f.read_input(business_id)
        input_business_id = f.get_business_by_id(input_business_id)
    else:
        input_business_id, content_score, score = "", "", ""
    image = f.get_business_image(business_id)
    business = f.get_business_by_id(business_id)
    affluences = f.get_affluence(business_id)
    reviews = f.get_reviews(business_id)

    if len(affluences) != 0:
        affluence_hours = sorted(affluences[0]['_source']['checkin_per_hour'].items(), key=lambda x: int(x[0]))
        affluence_days = affluences[0]['_source']['checkin_per_day']
        affluence_months = affluences[0]['_source']['checkin_per_month']
        affluence_years = sorted(affluences[0]['_source']['checkin_per_year'].items(), key=lambda x: int(x[0]))
        mapping = {'January': 0, 'February': 1, 'March': 2, 'April': 3, 'May': 4, 'June': 5, 'July': 6, 'August': 7,
                   'September': 8, 'October': 9, 'November': 10, 'December': 11}
        affluence_months = sorted(affluence_months.items(), key=lambda x: mapping[x[0]])
    else:
        affluence_hours = {}
        affluence_days = {}
        affluence_months = {}
        affluence_years = {}

    return render(request, 'pages/business.html',
                  {'input_business_id': input_business_id, 'content_score': content_score, 'score': score,
                   'business': business, 'image': image,
                   'affluences_days': affluence_days,
                   'affluences_hours': affluence_hours,
                   'affluences_months': affluence_months,
                   'affluences_years': affluence_years,
                   'reviews': reviews})
