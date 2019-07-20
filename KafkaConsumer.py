from kafka import KafkaConsumer
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext, Row
#connect to Hive
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")
#create spark session
spark = SparkSession.builder.appName('StreamingFromKafkaToHive').enableHiveSupport().getOrCreate()


#create kafka consumer and assign a new group
consumer = KafkaConsumer('test',group_id = 'newgroup', bootstrap_servers='localhost:6667')
#read the message
for msg in consumer:
    #decode the message
    message = msg.value.decode('utf-8')
    print(message)
    #create dataframe from json file
    df = spark.createDataFrame([Row(json=message)])
    new_df = spark.read.json(df.rdd.map(lambda r: r.json))
    #create temptable and write to hive
    new_df.createOrReplaceTempView("tempTable")
    sqlContext.sql("insert into table tweets select * from tempTable")