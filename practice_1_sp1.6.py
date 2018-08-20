# This is to practice Spark DataFrames, the data is from the Kaggle Credit Card competition. It can be
# either downloaded from Kaggle or from my Dropbox :
# https://www.dropbox.com/s/29x1w744mryi8l7/creditcard.csv.zip?dl=0
# Under the HortonWorks Sandbox and login as maria_dev/maria_dev create the folder hdpcd:
#     hdfs dfs -mkdir hdpcd
# unzip the file and from where the file is type :
#     hdfs dfs -put creditcard.csv hdpcd
# Now the file is uploaded to HDFS, we could have skipped this step if we wanted to.
# ----- We can use the housing csv data as well (much simpler)
#

# this is specific to spark 1.6
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.csv("/user/maria_dev/housing.csv")

df.printSchema()

df.head()

df.count()

# calculate statistics min, max, mean,... for numerical variables
df.describe().show()

# shows the 5 first observations
df.show(5)

# count when we drop null values
df.dropna().count()


