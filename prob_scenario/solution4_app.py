#
# ---- pyspark app
#
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("myApp")
sc   = SparkContext(conf = conf)

# loading from local dirctory, we could upload to hdfs. file:/ also works
# 1 -
rdd1 = sc.textFile("file:///home/maria_dev/kamel/Spark_Training/prob_scenario/hadoopexama4A.txt")
rdd2 = sc.textFile("file:///home/maria_dev/kamel/Spark_Training/prob_scenario/hadoopexama4B.txt")
rdd3 = sc.textFile("file:///home/maria_dev/kamel/Spark_Training/prob_scenario/hadoopexama4C.txt")

# 2 -
rddAll = rdd1.union(rdd2).union(rdd3)

# 3 - 
rdd4 = rddAll.flatMap(lambda x : x.split(" "))
print "There number of words is %i " % rdd4.count()

