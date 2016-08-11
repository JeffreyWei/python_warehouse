__author__ = 'wei'

import os
import sys
os.environ['SPARK_HOME']="/Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7"
sys.path.append("/Users/wei/work/tools/spark-2.0.0-bin-hadoop2.7/python")

try:
	from pyspark import SparkContext,SparkConf
	print("Successfully imported Spark Modules")
except ImportError as e:
	print("Can not import Spark Modules", e)
	sys.exit(1)
