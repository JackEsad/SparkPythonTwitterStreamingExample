from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import threading

#consumer key, consumer secret, access token, access secret.
ckey=""
csecret=""
atoken=""
asecret=""

class listener(StreamListener):
    def on_data(self, data):
        with io.open("/home/jack/Desktop/tweet/input.txt", "a+") as file:
            file.write(data)

        return(True)

    def on_error(self, status):
        print (status)

def doTwitterStreaming():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=["obama"]) 

t = threading.Thread(target=doTwitterStreaming)
try:
    
    t.start()

    sc = SparkContext(appName="Comp6360Project")
    ssc = StreamingContext(sc, 10)
    lines = ssc.textFileStream("/home/jack/Desktop/tweet/input.txt")
    words = lines.flatMap(lambda line: json.loads(line)["text"].split(" "))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    langs = lines.map(lambda line: (json.loads(line)["lang"],1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    locs = lines.map(lambda line: (json.loads(line)["user"]["location"],1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    words.saveAsTextFiles("output/word","")
    langs.saveAsTextFiles("output/langs","")
    locs.saveAsTextFiles("output/locs","")

    ssc.start() 
    ssc.awaitTermination()

finally:
    print("end")
    
