__author__ = 'wei'


import os
import sys
os.environ['SPARK_HOME']="/Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7"
sys.path.append("/Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7/python")


from pyspark import SparkContext, SparkConf
logFile = "/Users/wei/Desktop/log"
conf = SparkConf().setAppName("Simple App").setMaster("local")
sc = SparkContext(conf=conf)
# sc = SparkContext("local", "Simple App")
text_file = sc.textFile(logFile)
counts = text_file.flatMap(lambda line: line.split(" ")) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("wc_1")
