import json
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from cassandra.cluster import Cluster


def connect_to_cassandra():
    cluster= Cluster(['localhost'],port=9042)
    session= cluster.connect("twitt")
    session.execute("CREATE KEYSPACE IF NOT EXISTS twitter WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS twitter.twitts (day int, hour int,minute int, twitt_id text, PRIMARY KEY ((day, hour), minute, twitt_id))")
    session.execute("CREATE TABLE IF NOT EXISTS twitter.persons (user_name text, day int, hour int, twitt_id text, PRIMARY KEY(user_name, day, hour, text_id))")
    session.execute("CREATE TABLE IF NOT EXISTS twitter.hashtags (hashtag text, tdate date , twitt_id text, PRIMARY KEY (hashtag, tdate , twitt_id))")

    return session, cluster

add_to_twitts="INSERT INTO twitts (day, hour,minute ,twitt_id) VALUES (%s, %s , %s) "
add_to_persons="INSERT INTO persons (user_name, day, hour, twitt_id) VALUES (%s, %s, %s, %s)"
add_to_hashtags="INSERT INTO hashtags (hashtag, tdate, twitt_id) VALUES (%s, %s, %s)"

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer('channel-history')
    session, cluster= connect_to_cassandra()
    session.set_keyspace('twitter')
    while consumer:
        consumer = KafkaConsumer('channel-history')
        for msg in consumer:
            data = json.loads(msg.value)
            time1= data["ptime"]
            hour, minute, second = [int(x) for x in time1.split(':')]
            date1 = data["pdate"]
            year,month,day = [int(x) for x in time1.split('-')]

            session.execute(add_to_twitts, (day, hour,minute, data["id"]))
            session.execute(add_to_persons, (data["user"], day, hour, data["id"]))
            if data["hashtags"]!=[]:
                for hashtag in data["hashtags"]:
                    session.execute(add_to_hashtags, (hashtag, data["date"], data["id"]))

            print(data['text'])
            producer.send('statistics', value=data)
            print("4 -> channel-history send to statistics")
