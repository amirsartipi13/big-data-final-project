import stanza, uuid, re, json, datetime, requests, os, yake
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from time import sleep
from hazm import *

# stanza.download('fa')
# nlp = stanza.Pipeline('fa')

class DefaultEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, type(uuid.uuid4())):
            return obj.hex
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


with open('./files/stop_wrods.txt', 'r', encoding="utf-8") as f:
    stop_words = list(set([x.rstrip() for x in f]))

special_keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت',
                   'حسن روحانی', 'دلار', 'طلا', 'کرونا', 'انتخابات', 'دانشگاه', 'تورم']

covid_keywords = ['کوید19', 'کووید19', 'کوید', 'کووید19']

static_keywords = special_keywords + covid_keywords

def write_json(new_data, filename='./files/data.json'):
    with open(filename,'r+') as file:
        file_data = json.load(file)
        file_data["data"].append(new_data)
        file.seek(0)
        json.dump(file_data, file,cls=DefaultEncoder, indent = 4)

def find_keywords(message, kw):
	kw_extractor = yake.KeywordExtractor()
	custom_kw_extractor = yake.KeywordExtractor(n=1, features=None, top=10)
	keywords = custom_kw_extractor.extract_keywords(message)
	keywords = [x[0] for x in keywords if x[1] > 0.09]  + kw
	return list(set(keywords))

def pre_process(tweet):
    id = uuid.uuid4()
    text = tweet['text']
    normalizer = Normalizer()
    date = datetime.datetime.now()
    pdate, ptime, ptimestamp = split_date_time(tweet['created_at'])
    urls = list(set(re.findall("(?P<url>https?://[^\s]+)", text)))
    text = re.sub(r'[A-Za-z0-9]+@[a-zA-z].[a-zA-Z]+', '', text)
    text = text.split(':')
    text = ":".join(text[-1:])
    hashtags = list(set(re.findall(r"#(\w+)", text)))
    text = normalizer.normalize(text)
    text = word_tokenize(text)
    text = [word for word in text if word not in stop_words and word.isalpha()]
    keywords = [word for word in text if word in static_keywords]
    text = ' '.join(text)
    keywords = find_keywords(text, keywords)
    return {
        "id": id,
        "text":tweet['text'],
        "user": tweet['user']['screen_name'],
        "ctext":text,
        "pdate":pdate,
        "ptime":ptime,
        "ptimestamp": ptimestamp,
        "date":date,
        "created_at":tweet['created_at'],
        "hashtags":hashtags,
        "urls":urls,
        "keywords":keywords
    }



def split_date_time(date):
    date = date.split(' ')
    year = date[-1]
    day = date[2]
    month = datetime.datetime.strptime(date[1], "%b").month
    time = date[3]
    date = str(year) + '-' + str(month) + '-' + str(day)
    date_time = date + ' ' + time
    timestamp = datetime.datetime.timestamp(datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"))
    return date, time, timestamp


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer('pre-process')
    while True:
        sleep(3)
        consumer = KafkaConsumer('pre-process')
        for msg in consumer:
            tweet = json.loads(msg.value)
            tweet = pre_process(tweet)
            write_json(tweet)
            jtweet = json.dumps(tweet, cls=DefaultEncoder)
            print("______________________________")
            producer.send('persistence', value=jtweet)
            print("2 -> pre-process send to persistence")
    

