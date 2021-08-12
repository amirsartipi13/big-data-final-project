from flask import Flask, jsonify, render_template, request
import json, time, logging, redis
from json import loads, dumps



app = Flask(__name__)
r = redis.Redis(host='127.0.0.1', port=6379)

@app.route("/")
def index():
    data = [
        {
        "url": "http://127.0.0.1:5000/time-filter/?start=2021-8-11-00&end=2021-8-13-00",
        "name": "time-filter"
    },{
        "url": "http://127.0.0.1:5000/keywords",
        "name": "last_6_hour_keywords"
    },
    {
        "url": "http://127.0.0.1:5000/last-1000-hashtag",
        "name": "get_last_hashtags"
    },
    {
        "url": "http://127.0.0.1:5000/last-hour-hashtag",
        "name": "last-hour-hashtag"
    },
    {
        "url": "http://127.0.0.1:5000/last-tweets-100",
        "name": "get_last_tweets"
    }
    ]
    return render_template('index.html', data=data)


@app.route('/time-filter/',methods=['GET'])
def get_tweets_by_time():
    start = [int(x) for x in request.args.get("start").split("-")]
    end = [int(x) for x in request.args.get("end").split("-")]
    date = end.copy()
    query_dates = []
    while start <= date:
        query_dates.append(":".join(["{:02d}".format(x) for x in date]))
        app.logger.info(str(date))
        date[0] = date[0] if (date[1] != 1 or date[2] !=
                              1 or date[3] != 0) else date[0] - 1
        date[1] = date[1] if (date[2] != 1 or date[3] !=
                              0) else date[1] - 1 if date[1] > 1 else 12
        date[2] = date[2] if date[3] != 0 else date[2] - \
            1 if date[2] > 1 else 30
        date[3] = date[3] - 1 if date[3] > 0 else 24
    data = []
    # query_dates = [q[-8:]for q in query_dates]
    for d in query_dates:
        data.extend(r.lrange(f"tweets:{d}", 1, -1))

    data = [loads(x.decode()) for x in data]
    return render_template('tweets.html', data=data)



@app.route('/keywords')
def last_6_hour_keywords():
    keywords = [x.decode().split(":")[1] for x in r.keys("keyword:*")]
    keywords_counts = {}
    for k in keywords:
        keywords_counts[k] = keywords.count(k)
    return render_template('last_6_hour_keywords.html', data=keywords_counts)


@app.route('/last-1000-hashtag')
def get_last_hashtags():
    data = [x.decode() for x in r.lrange("last_hashtags", 0, -1)]
    return render_template('last_hour_hashtag.html', data=data)

@app.route('/last-hour-hashtag')
def get_last_hour_hashtags():
    keys = [x.decode().split(":")[1] for x in r.keys("last_hour_hashtags:*")]
    return render_template('last_hour_hashtag.html', data=keys)


@app.route('/last-tweets-100')
def get_last_tweets():
    data = [loads(x.decode()) for x in r.lrange("last_tweets", 0, -1)]
    return render_template('last_100_tweet.html', data=data)


if __name__ == '__main__':   
    app.run(debug=True)