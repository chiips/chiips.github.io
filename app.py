# -*- coding: utf-8 -*-
from redis import Redis
redis = Redis()

import os
import time
from functools import update_wrapper
from flask import request, g
from flask import Flask, jsonify
import sqlalchemy

# web app
app = Flask(__name__)

# database engine
engine = sqlalchemy.create_engine(os.getenv('SQL_URI'))

#rate limiter
class RateLimit(object):
    expiration_window = 10 #give redis key extra 10 seconds to expire in case of poor synchronization with redis server

    def __init__(self, key_prefix, limit, per, send_x_headers):
        self.reset = (int(time.time()) // per) * per + per #timestamp for when request limit can reset itself
        self.key = key_prefix + str(self.reset) #append reset timestamp to key, the string for tracking request
        self.limit = limit #number of requests within given time period (per)
        self.per = per
        self.send_x_headers = send_x_headers #to send request count remaining before hitting limit
        p = redis.pipeline() #pipeline to send multiple commands to redis
        p.incr(self.key) #increment count value at this key in redis
        p.expireat(self.key, self.reset + self.expiration_window) #set key expiry in redis
        self.current = min(p.execute()[0], limit) #get min of limit or response from first call to redis pipeline (incr)

    remaining = property(lambda x: x.limit - x.current) #how many requests left?
    over_limit = property(lambda x: x.current >= x.limit) #requests over limit?


@app.route('/')
def index():
    return 'Welcome to EQ Works 😎'


@app.route('/events/hourly')
def events_hourly():
    return queryHelper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/events/daily')
def events_daily():
    return queryHelper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/stats/hourly')
def stats_hourly():
    return queryHelper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/stats/daily')
def stats_daily():
    return queryHelper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/poi')
def poi():
    return queryHelper('''
        SELECT *
        FROM public.poi;
    ''')

def queryHelper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])
