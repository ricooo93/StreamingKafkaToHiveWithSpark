import tweepy
import kafka
import json

#set the OAuth parameters for twitter API
access_token = ""
access_token_secret =  ""
consumer_key =  ""
consumer_secret =  ""

#function that cleans the tweet json file
def cleanup(original):
    data = {}
    #collect the text, id and create time for the tweet, drop the rest
    for item in ['text','id','created_at']:
        data[item] = original[item]
    return json.dumps(data)

#create a twitter stream listener
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        processed = cleanup(json.loads(data))
        #publish the tweet to kafka topic 'test'
        producer.send("test", processed.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

#set up the kafka producer
producer = kafka.KafkaProducer(bootstrap_servers='localhost:6667')
#create stream listener
l = StdOutListener()
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = tweepy.Stream(auth, l)
#restrict the tweets to Melbourne area
stream.filter(locations=[112.921114,-43.740482,159.109219,-9.142176])