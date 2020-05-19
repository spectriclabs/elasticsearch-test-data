#!/usr/bin/python

import json
import time
import logging
import random
import string
import uuid
import datetime
import csv
import os
import math

import tornado.gen
import tornado.httpclient
import tornado.ioloop
import tornado.options

async_http_client = tornado.httpclient.AsyncHTTPClient()
headers = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
id_counter = 0
upload_data_count = 0
_dict_data = None
_cities_data = None
last_geo_point = (0,0)
last_heading = 0.0
last_speed = 0.0
last_interval = 0.0
last_time = 0
last_string =""

byte_range = (-128, 127)
short_range = (-32768, 32767)
integer_range = (-2**31, 2**31-1)
long_range = (-2**63, 2**63-1)

def delete_index(idx_name):
    try:
        url = "%s/%s" % (tornado.options.options.es_url, idx_name)
        request = tornado.httpclient.HTTPRequest(url, headers=headers, method="DELETE", request_timeout=240, auth_username=tornado.options.options.username, auth_password=tornado.options.options.password, validate_cert=tornado.options.options.validate_cert)
        response = tornado.httpclient.HTTPClient().fetch(request)
        logging.info('Deleting index  "%s" done   %s' % (idx_name, response.body))
    except tornado.httpclient.HTTPError:
        pass

def create_index(idx_name, format):
    schema = {
        "settings": {
            "number_of_shards":   tornado.options.options.num_of_shards,
            "number_of_replicas": tornado.options.options.num_of_replicas
        }
    }

    if not tornado.options.options.dynamic_index:
        schema["mappings"] = generate_mapping(format)

    body = json.dumps(schema)
    url = "%s/%s" % (tornado.options.options.es_url, idx_name)
    try:
        logging.info('Trying to create index %s' % (url))
        request = tornado.httpclient.HTTPRequest(url, headers=headers, method="PUT", body=body, request_timeout=240, auth_username=tornado.options.options.username, auth_password=tornado.options.options.password, validate_cert=tornado.options.options.validate_cert)
        response = tornado.httpclient.HTTPClient().fetch(request)
        logging.info('Creating index "%s" done   %s' % (idx_name, response.body))
    except tornado.httpclient.HTTPError:
        logging.info('Looks like the index exists already')
        pass


@tornado.gen.coroutine
def upload_batch(upload_data_txt):
    try:
        request = tornado.httpclient.HTTPRequest(tornado.options.options.es_url + "/_bulk",
                                                 method="POST",
                                                 body=upload_data_txt,
                                                 headers=headers,
                                                 request_timeout=tornado.options.options.http_upload_timeout,
                                                 auth_username=tornado.options.options.username, auth_password=tornado.options.options.password, validate_cert=tornado.options.options.validate_cert)
        response = yield async_http_client.fetch(request)
    except Exception as ex:
        logging.error("upload failed, error: %s" % ex)
        return

    result = json.loads(response.body.decode('utf-8'))
    res_txt = "OK" if not result['errors'] else "FAILED"
    took = int(result['took'])
    logging.info("Upload: %s - upload took: %5dms, total docs uploaded: %7d" % (res_txt, took, upload_data_count))

def get_mapping_for_format(format):
    split_f = format.split(":")
    if not split_f:
        return None, None

    field_name = split_f[0]
    field_type = split_f[1]

    field_mapping = {}
    if field_type == "bool":
        field_mapping["type"] = "boolean"

    elif field_type in ("str","str_series"):
        field_mapping["type"] = "keyword"

    elif field_type == "int":
        min = 0 if len(split_f) < 3 else int(split_f[2])
        max = min + 100000 if len(split_f) < 4 else int(split_f[3])

        if ((byte_range[0] <= min <= byte_range[1]) and
            (byte_range[0] <= max <= byte_range[1])):
            field_mapping["type"] = "byte"
        elif ((short_range[0] <= min <= short_range[1]) and
              (short_range[0] <= max <= short_range[1])):
            field_mapping["type"] = "short"
        elif ((integer_range[0] <= min <= integer_range[1]) and
              (integer_range[0] <= max <= integer_range[1])):
            field_mapping["type"] = "integer"
        elif ((long_range[0] <= min <= long_range[1]) and
              (long_range[0] <= max <= long_range[1])):
            field_mapping["type"] = "long"

    elif field_type == "ipv4":
        field_mapping["type"] = "ip"

    elif field_type in ("ts", "ts_series"):
        field_mapping["type"] = "date"
        field_mapping["format"] = "epoch_millis"

    elif field_type == "tstxt":
        field_mapping["type"] = "date"
        field_mapping["format"] = "strict_date_time"

    elif field_type in ("words", "dict", "text"):
        field_mapping["type"] = "text"

    elif field_type in ("geo_point", "cities","cities_path_series"):
        field_mapping["type"] = "geo_point"
    
    elif field_type in ("ellipse","ellipsecities","path"):
        field_mapping["type"] = "geo_shape"

    else:
        field_mapping["type"] = field_type

    return field_name, field_mapping 

def get_data_for_format(format,doc_num):
    split_f = format.split(":")
    if not split_f:
        return None, None

    field_name = split_f[0]
    field_type = split_f[1]

    return_val = ''
    global _cities_data

    if field_type == "bool":
        return_val = random.choice([True, False])

    elif field_type == "str":
        min = 3 if len(split_f) < 3 else int(split_f[2])
        max = min + 7 if len(split_f) < 4 else int(split_f[3])
        length = generate_count(min, max)
        return_val = "".join([random.choice(string.ascii_letters + string.digits) for x in range(length)])

    elif field_type == "str_series":
        min = 3 if len(split_f) < 3 else int(split_f[2])
        max = min + 7 if len(split_f) < 4 else int(split_f[3])
        interval = 60 if len(split_f) < 5 else int(split_f[4])
        length = generate_count(min, max)
        global last_string
        if (doc_num % interval == 0):
            return_val = "".join([random.choice(string.ascii_letters + string.digits) for x in range(length)])
            last_string = return_val
        else:
            return_val = last_string

    elif field_type == "int":
        min = 0 if len(split_f) < 3 else int(split_f[2])
        max = min + 100000 if len(split_f) < 4 else int(split_f[3])
        return_val = generate_count(min, max)

    elif field_type in ("float", "double", "half_float"):
        min = 0.0 if len(split_f) < 3 else int(split_f[2])
        max = min + 100000.0 if len(split_f) < 4 else int(split_f[3])
        return_val = generate_float(min, max)
    
    elif field_type == "ipv4":
        return_val = "{0}.{1}.{2}.{3}".format(generate_count(0, 245),generate_count(0, 245),generate_count(0, 245),generate_count(0, 245))

    elif field_type == "ts":
        now = int(time.time())
        per_day = 24 * 60 * 60
        min_time = now - per_day * (30 if len(split_f) < 3 else int(split_f[2]))
        max_time = now + per_day * (30 if len(split_f) < 4 else int(split_f[3]))
        ts = generate_count(min_time, max_time)
        return_val = int(ts * 1000)

    elif field_type == "ts_series":
        global last_time
        now = int(time.time())
        per_day = 24 * 60 * 60
        min_time = now - per_day * (30 if len(split_f) < 3 else int(split_f[2]))
        max_time = now + per_day * (30 if len(split_f) < 4 else int(split_f[3]))
        delta = 60000 if len(split_f) < 5 else int(split_f[4])
        interval = 60 if len(split_f) < 6 else int(split_f[5])
        ts = 0
        if (doc_num % interval ==0):
            ts = generate_count(min_time, max_time)*1000
        else:
            ts = last_time
        new_time = int(ts+delta)
        last_time = new_time
        return_val = new_time

    elif field_type == "tstxt":
        now = int(time.time())
        per_day = 24 * 60 * 60
        min = now - 30 * per_day if len(split_f) < 3 else datetime.datetime.timestamp(datetime.datetime.strptime(split_f[2], "%Y-%m-%dT%H-%M-%S"))
        max = now + 30 * per_day if len(split_f) < 4 else datetime.datetime.timestamp(datetime.datetime.strptime(split_f[3], "%Y-%m-%dT%H-%M-%S"))
        ts = generate_count(min, max)
        return_val = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S.000-0000")


    elif field_type == "words":
        min = 2 if len(split_f) < 3 else int(split_f[2])
        max = min + 8 if len(split_f) < 4 else int(split_f[3])
        count = generate_count(min, max)
        words = []
        for _ in range(count):
            word_len = random.randrange(3, 10)
            words.append("".join([random.choice(string.ascii_letters + string.digits) for x in range(word_len)]))
        return_val = " ".join(words)

    elif field_type == "dict":
        global _dict_data
        min = 2 if len(split_f) < 3 else int(split_f[2])
        max = min + 8 if len(split_f) < 4 else int(split_f[3])
        count = generate_count(min, max)
        return_val = " ".join([random.choice(_dict_data).strip() for _ in range(count)])

    elif field_type == "text":
        text = ["text1", "text2", "text3"] if len(split_f) < 3 else split_f[2].split("-")
        min = 1 if len(split_f) < 4 else int(split_f[3])
        max = min + 1 if len(split_f) < 5 else int(split_f[4])
        count = generate_count(min, max)
        words = []
        for _ in range(count):
            words.append(""+random.choice(text))
        return_val = " ".join(words)

    elif field_type == "geo_point":
        min_lat =  -90.0 if len(split_f) < 3 else float(split_f[2])
        max_lat =   90.0 if len(split_f) < 4 else float(split_f[3])
        min_lon = -180.0 if len(split_f) < 5 else float(split_f[4])
        max_lon =  180.0 if len(split_f) < 6 else float(split_f[5])

        return_val = {
            "lat": random.uniform( min_lat,  max_lat ),
            "lon": random.uniform( min_lon,  max_lon )
        }


    elif field_type == "cities":
        if not _cities_data:
            logging.error("cannot generate cities data without cities file, see README.md")
            exit(1)
        min_radius_meters = 0.0 if len(split_f) < 3 else float(split_f[2])
        max_radius_meters = (min_radius_meters + 10000) if len(split_f) < 4 else float(split_f[3])

        chosen_city = random.choice(_cities_data)
        point = generate_random_point(
            float(chosen_city["lng"]),float(chosen_city["lat"]),
            min_radius_meters, max_radius_meters
        )

        return_val = {
            "lon": point[0],
            "lat": point[1]
            
        }

    elif field_type == "cities_path_series":

        path_length = 100 if len(split_f) < 3 else int(split_f[2])
        min_radius_meters = 0.0 if len(split_f) < 4 else float(split_f[3])
        max_radius_meters = (min_radius_meters + 10000) if len(split_f) < 5 else float(split_f[4])
        heading_std = 5.0 if len(split_f) < 6 else float(split_f[5])
        speed_start = 1000.0 if len(split_f) < 7 else float(split_f[6])
        speed_std = 50.0 if len(split_f) < 8 else float(split_f[7])
        interval_start = 60.0 if len(split_f) < 9 else float(split_f[8])
        interval_std = 5.0 if len(split_f) < 10 else float(split_f[9])
        point =[0,0]

        global last_geo_point
        global last_heading
        global last_interval
        global last_speed

        if doc_num % path_length == 0: #First Item of a new path
            if not _cities_data:
                logging.error("cannot generate cities data without cities file, see README.md")
                exit(1)

            min_radius_meters = 0 if len(split_f) < 4 else float(split_f[3])
            max_radius_meters = (min_radius_meters + 10000) if len(split_f) < 5 else float(split_f[4])

            chosen_city = random.choice(_cities_data)
            point = generate_random_point(
                float(chosen_city["lng"]),float(chosen_city["lat"]),
                min_radius_meters, max_radius_meters
            )

            last_speed = speed_start
            last_heading = random.uniform(0,360)
            last_interval = interval_start
            last_geo_point = point

        else:
            distance = last_speed *last_interval 
            point = generate_next_geo_point(last_geo_point,last_heading,distance)
            last_speed = last_speed + random.gauss(0,speed_std)
            last_heading = last_heading + random.gauss(0,heading_std)
            last_heading = last_heading % 360
            last_interval = last_interval + random.gauss(0,interval_std)
            if (last_interval<1):
                last_interval = 1 
            last_geo_point = point

        return_val = {
            "lon": point[0],
            "lat": point[1]
            
        }
        


    elif field_type == "ellipse":

        ellipse_maj_mean = 0.2 if len(split_f) < 3 else float(split_f[2])
        ellipse_min_mean = 0.1 if len(split_f) < 4 else float(split_f[3])
        ellipse_maj_std  = .05 if len(split_f) < 5 else float(split_f[4])
        ellipse_min_std  = .05 if len(split_f) < 6 else float(split_f[5])
        ellipse_num_points = 30 if len(split_f) < 7 else int(split_f[6])

        # Random Center Point
        point1 = random.uniform( -180,  180 )
        point2 = random.uniform( -90,  90 )
        
        points = generate_random_ellipse(point1,point2,ellipse_maj_mean,ellipse_min_mean,ellipse_maj_std,ellipse_min_std,ellipse_num_points)
        return_val = {
                "type" : "polygon",
                "coordinates": [points]
            }
    elif field_type == "ellipsecities":

        ellipse_maj_mean = 0.2 if len(split_f) < 3 else float(split_f[2])
        ellipse_min_mean = 0.1 if len(split_f) < 4 else float(split_f[3])
        ellipse_maj_std  = .05 if len(split_f) < 5 else float(split_f[4])
        ellipse_min_std  = .05 if len(split_f) < 6 else float(split_f[5])
        ellipse_num_points = 30 if len(split_f) < 7 else int(split_f[6])

        if not _cities_data:
            logging.error("cannot generate cities data without cities file, see README.md")
            exit(1)
        sigma_degrees = 0.01 if len(split_f) < 8 else float(split_f[7])

        chosen_city = random.choice(_cities_data)
        point = generate_random_point_normal(
            float(chosen_city["lng"]), float(chosen_city["lat"]),sigma_degrees)
        point1 = point[0]
        point2 = point[1]
        
        points = generate_random_ellipse(point1,point2,ellipse_maj_mean,ellipse_min_mean,ellipse_maj_std,ellipse_min_std,ellipse_num_points)
        return_val = {
                "type" : "polygon",
                "coordinates": [points]
            }
        
        # return_val = {
        #         "type" : "geometrycollection", 
        #         "geometries": [
        #             {
        #             "type" : "point",
        #             "coordinates": [point[1],point[0]] 
        #             },
        #             {
        #             "type" : "polygon",
        #             "coordinates": [points] 
        #             }
        #         ] 
        # }        

    elif field_type == "path":
            
        # Random Center Point
        start_lon = random.uniform( -180,  180 )
        start_lat = random.uniform( -90,  90 )

        length = 20 if len(split_f) < 3 else int(split_f[2])
        heading_std = 5.0 if len(split_f) < 4 else float(split_f[3])
        speed_start = 1000.0 if len(split_f) < 5 else float(split_f[4])
        speed_std = 50.0 if len(split_f) < 6 else float(split_f[5])
        
        points = generate_random_path(start_lon,start_lat,length,heading_std,speed_start,speed_std)

        return_val = {
            "type" : "linestring",
            "coordinates": points
        }

    return field_name, return_val

def generate_float(min, max):
    if min == max:
        return max
    elif min > max:
        return random.uniform(max, min);
    else:
        return random.uniform(min, max);

def generate_count(min, max):
    if min == max:
        return max
    elif min > max:
        return random.randrange(max, min);
    else:
        return random.randrange(min, max);


def generate_random_doc(format,doc_num):
    global id_counter

    res = {}

    for f in format:
        f_key, f_val = get_data_for_format(f,doc_num)
        if f_key:
            res[f_key] = f_val

    if not tornado.options.options.id_type:
        return res

    if tornado.options.options.id_type == 'int':
        res['_id'] = id_counter
        id_counter += 1
    elif tornado.options.options.id_type == 'uuid4':
        res['_id'] = str(uuid.uuid4())

    return res

def generate_random_point(lon_dd, lat_dd, min_radius_meters, max_radius_meters):
    """
    From https://jordinl.com/posts/2019-02-15-how-to-generate-random-geocoordinates-within-given-radius
    """
    lat_rad = lat_dd * ( math.pi / 180.0)
    lon_rad = lon_dd * ( math.pi / 180.0)

    earth_radius = 6371000.0

    u = (max_radius_meters ** 2) - (min_radius_meters ** 2)
    distance = math.sqrt((random.random() * u) + (min_radius_meters ** 2))
    distance_over_er = (distance / earth_radius)

    delta_lat = math.cos(random.random() * math.pi) * distance_over_er
    sign = random.choice((-1, 1))

    v = math.cos(distance_over_er) - math.cos(delta_lat)
    x = math.cos(lat_rad) * math.cos(delta_lat + lat_rad)
    delta_lon = sign * math.acos(( v / x ) + 1)

    ans_lat_dd = (lat_rad + delta_lat) * (180.0 / math.pi)
    ans_lon_dd = (lon_rad + delta_lon) * (180.0 / math.pi)

    return ans_lon_dd, ans_lat_dd

def generate_random_point_normal(lon_dd, lat_dd, sigma_degrees):

    ans_lat_dd = lat_dd+random.gauss(0,sigma_degrees)/2
    ans_lon_dd = lon_dd+random.gauss(0,sigma_degrees)
    
    if lon_dd<-180: lon_dd=-180
    if lon_dd>180: lon_dd=180
    if ans_lat_dd<-90: ans_lat_dd=-90
    if ans_lat_dd>90: ans_lat_dd=90
    
    return ans_lat_dd, ans_lon_dd

def generate_random_ellipse(x,y,ellipse_maj_mean,ellipse_min_mean,ellipse_maj_std,ellipse_min_std,ellipse_num_points):

    # Rotation of ellipse is uniform from 0 to 180 degrees
    rotation =  random.uniform(0,180)
    rotation = rotation / 180 * math.pi;

    # Major and Minor Ellipse Lengths are created from a normal distribution based on mean, and std specified
    a = random.gauss(ellipse_maj_mean,ellipse_maj_std)
    b = random.gauss(ellipse_min_mean,ellipse_min_std)

    points = []
    # Loop over number of points, and compute ellipse points
    for i in range(ellipse_num_points): 
        theta = math.pi*2/ellipse_num_points*i + rotation
        r = a*b/math.sqrt(a*a*math.sin(theta)*math.sin(theta) + b*b*math.cos(theta)*math.cos(theta))
        x1 = x+math.cos(theta-rotation) * r
        y1 = y+math.sin(theta-rotation) * r
        if x1<-180: x1=-180
        if x1>180: x1=180
        if y1<-90: y1=-90
        if y1>90: y1=90
        points.append([x1,y1])
    points.append(points[0])
    return points

def generate_random_path(x,y,length,heading_std,speed_start,speed_std):

    earth_circ = 6371000.0 * 2 * math.pi

    points = []
    points.append([x,y])

    heading = random.uniform(0,360)
    speed = speed_start
    
    # Make 1 point every 60 seconds for a total of length points
    for i in range(length):
        heading = heading + random.gauss(0,heading_std)
        heading = heading % 360
        x1 = points[i][0]+ math.cos(math.radians(heading))*speed/earth_circ*360*60
        y1 = points[i][1]+ math.sin(math.radians(heading))*speed/earth_circ*360*60 
        if x1<-180: x1=-180
        if x1>180: x1=180
        if y1<-90: y1=-90
        if y1>90: y1=90
        points.append([x1,y1])
        speed = speed + random.gauss(0,speed_std)
    return points

def generate_next_geo_point(last_geo_point,last_heading,distance):

    earth_circ = 6371000.0 * 2 * math.pi
    x1 = last_geo_point[0]+ math.cos(math.radians(last_heading))*distance/earth_circ
    y1 = last_geo_point[1]+ math.sin(math.radians(last_heading))*distance/earth_circ
    if x1<-180: x1=-180
    if x1>180: x1=180
    if y1<-90: y1=-90
    if y1>90: y1=90
    return [x1,y1]


def generate_mapping(format):
    properties = {}
    for f in format:
        f_key, f_map = get_mapping_for_format(f)
        if (f_key != None) and (f_map != None):
            properties[f_key] = f_map
    return {"properties": properties }

def set_index_refresh(val):

    params = {"index": {"refresh_interval": val}}
    body = json.dumps(params)
    url = "%s/%s/_settings" % (tornado.options.options.es_url, tornado.options.options.index_name)
    try:
        request = tornado.httpclient.HTTPRequest(url, headers=headers, method="PUT", body=body, request_timeout=240, auth_username=tornado.options.options.username, auth_password=tornado.options.options.password, validate_cert=tornado.options.options.validate_cert)
        http_client = tornado.httpclient.HTTPClient()
        http_client.fetch(request)
        logging.info('Set index refresh to %s' % val)
    except Exception as ex:
        logging.exception(ex)

def load_cites(cities_file, num_of_cities):
    global _cities_data
    with open(tornado.options.options.cities_file, 'r') as f:
        _cities_data = list( csv.DictReader(f) )

    required_columns = ("city_ascii", "iso2", "lat", "lng")
    missing_columns = []
    for required_column in required_columns:
        if required_column not in _cities_data[0]:
            missing_columns.append(required_column)
        if missing_columns:
            logging.error("Cities data file missing columns '%s'", ",".join(missing_columns))
            exit(1)

    logging.info("Loaded %d cities from %s" % (len(_cities_data), tornado.options.options.dict_file))
    if num_of_cities:
        _cities_data = random.sample(_cities_data, num_of_cities)
        logging.info("Using %d cities from %s" % (len(_cities_data), tornado.options.options.dict_file))

@tornado.gen.coroutine
def generate_test_data():

    global upload_data_count

    format = tornado.options.options.format.split(',')
    if not format:
        logging.error('invalid format')
        exit(1)

    # Newer versions of ES are strict about extra '/' in the URL
    if tornado.options.options.es_url[-1] == '/':
        tornado.options.options.es_url = tornado.options.options.es_url[:-1]

    if tornado.options.options.force_init_index:
        delete_index(tornado.options.options.index_name)

    create_index(tornado.options.options.index_name, format)

    # todo: query what refresh is set to, then restore later
    if tornado.options.options.set_refresh:
        set_index_refresh("-1")

    if tornado.options.options.out_file:
        out_file = open(tornado.options.options.out_file, "w")
    else:
        out_file = None

    if tornado.options.options.dict_file:
        global _dict_data
        with open(tornado.options.options.dict_file, 'r') as f:
            _dict_data = f.readlines()
        logging.info("Loaded %d words from the %s" % (len(_dict_data), tornado.options.options.dict_file))

    if tornado.options.options.cities_file and os.path.exists(tornado.options.options.cities_file):
        load_cites(tornado.options.options.cities_file, tornado.options.options.num_of_cities)

    ts_start = int(time.time())
    upload_data_txt = ""
    total_uploaded = 0

    logging.info("Generating %d docs, upload batch size is %d" % (tornado.options.options.count,
                                                                  tornado.options.options.batch_size))
    for num in range(0, tornado.options.options.count):

        item = generate_random_doc(format,num)

        if out_file:
            out_file.write("%s\n" % json.dumps(item))

        cmd = {'index': {'_index': tornado.options.options.index_name,
                         '_type': tornado.options.options.index_type}}
        if '_id' in item:
            cmd['index']['_id'] = item['_id']

        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"
        upload_data_count += 1

        if upload_data_count % tornado.options.options.batch_size == 0:
            yield upload_batch(upload_data_txt)
            upload_data_txt = ""

    # upload remaining items in `upload_data_txt`
    if upload_data_txt:
        yield upload_batch(upload_data_txt)

    if tornado.options.options.set_refresh:
        set_index_refresh("1s")

    if out_file:
        out_file.close()

    took_secs = int(time.time() - ts_start)

    logging.info("Done - total docs uploaded: %d, took %d seconds" % (tornado.options.options.count, took_secs))


if __name__ == '__main__':
    tornado.options.define("es_url", type=str, default='http://localhost:9200/', help="URL of your Elasticsearch node")
    tornado.options.define("index_name", type=str, default='test_data', help="Name of the index to store your messages")
    tornado.options.define("index_type", type=str, default='_doc', help="Type")
    tornado.options.define("batch_size", type=int, default=1000, help="Elasticsearch bulk index batch size")
    tornado.options.define("num_of_shards", type=int, default=2, help="Number of shards for ES index")
    tornado.options.define("http_upload_timeout", type=int, default=3, help="Timeout in seconds when uploading data")
    tornado.options.define("count", type=int, default=100000, help="Number of docs to generate")
    tornado.options.define("format", type=str, default='name:str,age:int,last_updated:ts', help="message format")
    tornado.options.define("num_of_replicas", type=int, default=0, help="Number of replicas for ES index")
    tornado.options.define("force_init_index", type=bool, default=False, help="Force deleting and re-initializing the Elasticsearch index")
    tornado.options.define("dynamic_index", type=bool, default=False, help="Use dynamic index instead of a strict mapping")
    tornado.options.define("set_refresh", type=bool, default=False, help="Set refresh rate to -1 before starting the upload")
    tornado.options.define("out_file", type=str, default=False, help="If set, write test data to out_file as well.")
    tornado.options.define("id_type", type=str, default=None, help="Type of 'id' to use for the docs, valid settings are int and uuid4, None is default")
    tornado.options.define("dict_file", type=str, default=None, help="Name of dictionary file to use")
    tornado.options.define("cities_file", type=str, default="worldcities.csv", help="Name of dictionary file to use")
    tornado.options.define("num_of_cities", type=int, default=None, help="Number of cities to use when generating city geopoints, None is default")
    tornado.options.define("username", type=str, default=None, help="Username for elasticsearch")
    tornado.options.define("password", type=str, default=None, help="Password for elasticsearch")
    tornado.options.define("validate_cert", type=bool, default=True, help="SSL validate_cert for requests. Use false for self-signed certificates.")
    tornado.options.parse_command_line()

    tornado.ioloop.IOLoop.instance().run_sync(generate_test_data)
