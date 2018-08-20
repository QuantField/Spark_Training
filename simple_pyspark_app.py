# This initialisation is only valid for Spark 2.x
# from pyspark import SparkContext
# sc = SparkContext().getOrCreate()

# this is for spark 1.6
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("myApp")
sc   = SparkContext(conf = conf)

rdd = sc.parallelize([1,4,5,6,7])
sum = rdd.reduce(lambda x,y:x+y)
print "The sum is %i" % sum


# This is how to submit the application on the command line
# spark-submit simple_pyspark_app.py   
