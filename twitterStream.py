from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positives=[]
    negatives=[]
    for i in counts:
        for j in i:
            if j[0]=="positive":
                positives.append(j[1])
            else:
                negatives.append(j[1])
    
    x_ticks=range(0,12)
    
    lineP=plt.plot(positives)
    plt.setp(lineP, color='b', marker='o', label="positive")
    
    lineN=plt.plot(negatives)
    plt.setp(lineN, color='g', marker='o', label="negative")
   
    plt.margins(0.1)
    plt.ylabel("Word count")
    plt.xlabel("Time step")
    plt.xticks(x_ticks)
    
    plt.legend(loc=0)
    plt.show()  



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    f=open(filename)
    listToret=f.read().split()
    return listToret
    
   

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)
    

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
   
    words=tweets.flatMap(lambda x: x.split(" ")).filter(lambda x: x in pwords or x in nwords) 
    wordPairs=words.map(lambda x: ("positive",1) if x in pwords else ("negative",1))

    wordCount=wordPairs.reduceByKey(lambda x, y: x + y)
    
    runningCounts = wordPairs.updateStateByKey(updateFunction)

    runningCounts.pprint()
    

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    wordCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
