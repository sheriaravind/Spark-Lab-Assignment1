import os
os.environ["SPARK_HOME"] = r"V:\Softwares\Spark\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]= r"V:\Softwares\Spark"
from pyspark import SparkContext

def mapper(value):

    value = value.split("->")
    user = value[0]
    friends = value[1]
    friends2 = str(friends).replace(',','')
    friends1= friends.split(',')
    keys = []

    for friend in friends1:
        keys.append((''.join(sorted(user + friend)), friends2.replace(friend, "")))
    return keys

def reducer(key, value):
    routput = ''
    for friend in key:
        if friend in value:
            routput += friend
    return routput

if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    Lines = sc.textFile(r"C:\Users\aravi\OneDrive\Desktop\Sample.txt", 1)
    Line = Lines.flatMap(mapper)
    mutualFriends = Line.reduceByKey(reducer)
    mutualFriends.coalesce(1).saveAsTextFile("MutualFriends1")
    sc.stop()