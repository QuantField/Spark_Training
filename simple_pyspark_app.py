from pyspark import SparkContext
sc = SparkContext().getOrCreate()

rdd = sc.parallelize([1,4,5,6,7])
sum = rdd.reduce(lambda x,y:x+y)
print "The sum is %i" % sum


# This is how to submit the application on the command line
# spark-submit simple_pyspark_app.py   
