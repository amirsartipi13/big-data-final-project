import json
from json import loads, dumps
from cassandra.cluster import Cluster
from datetime import datetime, timezone

def connect_to_cassandra():
    cluster= Cluster(['localhost'],port=9042)
    session= cluster.connect("twitt")
    return session, cluster

if __name__ == '__main__':
    session, cluster= connect_to_cassandra()
    session.set_keyspace('twitter')

    #query1
    result1= session.execute("SELECT twitt_id FROM twitts where day=15 AND hour>=20 AND minute>30")
    for r1 in result1:
        print (r1)

    #query2
    result2= session.execute("SELECT twitt_id FROM persons where user_name='Rahmadzade' AND (day=15 OR (day=14 AND hour>21)")
    for r2 in result2:
        print (r2)

    #query3
    result3 = session.execute("SELECT twitt_id FROM hashtags where hashtag='help' AND date>'2021-8-13' AND date<'2021-8-15'")
    for r3 in result3:
        print(r3)

    #query4
    result4 = session.execute("SELECT twitt_id FROM person where user_name='Rahmadzade' day IN (8,15)")
    for r4 in result4:
        print(r4)