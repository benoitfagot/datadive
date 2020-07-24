from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
import datadive.functions as f
from math import radians, cos, sin, asin, acos, sqrt, pi, atan2


def get_geocoordinates_address(address):
    result = []
    geolocator = Nominatim(user_agent="Data_Dive")
    try:
        location = geolocator.geocode(address)
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
        try:
            location = geolocator.geocode(address, language="en", timeout=100)
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
            raise ('GeocoderServiceError occured')

    if location:
        lt = location.latitude
        lg = location.longitude
        gps = str(lt) + ',' + str(lg)
        location = geolocator.reverse(gps)
        response = location.raw
        result.append(float(response['lat']))
        result.append(float(response['lon']))
    else:
        print('location not found')

    return result


def calcul_dist(lat1, lng1, lat2, lng2):
    lat1 = lat1 * pi / 180
    lon1 = lng1 * pi / 180
    lat2 = lat2 * pi / 180
    lon2 = lng2 * pi / 180
    rads = acos(0.5 * ((1.0 + cos(lon1 - lon2)) * cos(lat1 - lat2) - (1.0 - cos(lon1 - lon2)) * cos(lat1 + lat2)))

    return 6378.388 * rads / 1000


def haversine_distance(lat1, lon1, lat2, lon2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formule
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers.

    return c * r / 1000


def get_neighbors(address):
    distances = list()
    neighbors = list()

    if address != "":
        coordGeoloc = get_geocoordinates_address(address)

    data = f.get_businesses(address)
    for cpt in range(len(data)):
        print(data[cpt]['_source'])
        latitude = data[cpt]['_source']['latitude']
        longitude = data[cpt]['_source']['longitude']

        dist = haversine_distance(coordGeoloc[0], coordGeoloc[1], latitude, longitude)
        distances.append((data[cpt]['_source'], dist))

    distances.sort(key=lambda tup: tup[1])
    print("Nombre d'élements : ", len(distances))
    for i in range(len(distances)):
        neighbors.append(distances[i][0])  # sert à rien à retirer ?
        print(neighbors[i])

    print('---------------')
    return neighbors


# do this in elastic

print(get_neighbors("Las Vegas"))
# print(f.home_businesses())
