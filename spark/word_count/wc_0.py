"""SimpleApp"""

from operator import add

from pyspark import SparkContext

# add bulid path
# PYTHONPATH   /Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7/python
# SPARK_HOME	/Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7

def tokenize(text):
	return text.split()


logFile = "/Users/wei/Desktop/log"
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()
words = logData.flatMap(tokenize)
wc = words.map(lambda x: (x, 1))
counts = wc.reduceByKey(add)
counts.saveAsTextFile("wc_0")

# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()

# print("Lines with a: %i, lines with b: %i"%(numAs, numBs))
