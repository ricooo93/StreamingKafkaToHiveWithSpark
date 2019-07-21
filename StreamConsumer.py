from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

sc = SparkContext.getOrCreate()
#set log level to warn to avoid too much information
sc.setLogLevel("WARN")
#set the window to 30 seconds
ssc = StreamingContext(sc, 30)
#set the zookeeper server, topic, and groupID
zkp, topic, group = 'localhost:2181', 'test', 'newgroup'
#create spark stream
kstream = KafkaUtils.createStream(ssc,zkp,topics={topic:1},groupId=group) 
#load the message
tweets = kstream.map(lambda x: json.loads(x[1]))
#print the number of tweets received
tweets.count().pprint()

ssc.start()
ssc.awaitTermination()