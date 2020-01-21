
import sys

from pyspark import SparkContext

sc = SparkContext()

filepath = "hdfs:" + sys.argv[1]

logfile=filepath

myflumedata = sc.textFile(logfile)

myjpgdata = myflumedata.filter(lambda x: ".jpg" in x)

for x in myjpgdata.take(10): print x


