
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Simple Test")\
        .config("spark.some.config.option","some-value")\
        .getOrCreate()


# df = spark.read.csv("file:/home/maria_dev/kamel/Spark_Training/housing.csv")
df = spark.read.csv("file:/home/maria_dev/kamel/Spark_Training/housing.csv",header = True) # first row = header
df.head()
Row(longitude=u'-122.23', latitude=u'37.88', housing_median_age=u'41.0', total_rooms=u'880.0', total_bedrooms=u'129.0', population=u'322.0', households=u'126.0', median_income=u'8.3252', median_house_value=u'452600.0', ocean_proximity=u'NEAR BAY')

>>> df.show(4)
+---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+
|longitude|latitude|housing_median_age|total_rooms|total_bedrooms|population|households|median_income|median_house_value|ocean_proximity|
+---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+
|  -122.23|   37.88|              41.0|      880.0|         129.0|     322.0|     126.0|       8.3252|          452600.0|       NEAR BAY|
|  -122.22|   37.86|              21.0|     7099.0|        1106.0|    2401.0|    1138.0|       8.3014|          358500.0|       NEAR BAY|
|  -122.24|   37.85|              52.0|     1467.0|         190.0|     496.0|     177.0|       7.2574|          352100.0|       NEAR BAY|
|  -122.25|   37.85|              52.0|     1274.0|         235.0|     558.0|     219.0|       5.6431|          341300.0|       NEAR BAY|
+---------+--------+------------------+-----------+--------------+----------+----------+-------------+------------------+---------------+
only showing top 4 rows

#---------- Now let's suppose that the file has no header ( I will create a file housing_nohead.csv without the first line)

rdd = sc.textFile("file:/home/maria_dev/kamel/Spark_Training/housing_nohead.csv")
#to create the column names :
from pyspark.sql import Row
df = rdd.map(lambda line : line.split(",")).map(lambda line : Row(      longitude = line[0],
									latitude  = line[1],
									housing_median_age = line[2],
									total_rooms = line[3],
									total_bedrooms = line[4],
									population = line[5],
									households = line[6],
									median_income = line[7],
									median_house_value = line[8],
									ocean_proximity = line[9]))


# last bit not working properly need to do something.
