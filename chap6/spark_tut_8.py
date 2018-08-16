# introduction to Spark Accumulators
# Accumulator is seed as an global variable it combine the restuls from the cluster nodes

#file = sc.textFile("testFile.txt")
# because we are in a hadoop environment we need the file:///
file = sc.textFile("file:///home/maria_dev/kamel/Spark_Training/chap6/testFile.txt")

blanklines = sc.accumulator(0)  # initialised with 0

def finBlanks(line):
    global blanklines
    if line=="":
        blanklines +=1
    return line.split(" ")

rdd = file.flatMap(finBlanks)

>>> rdd.first()
u'This'

rdd.collect() # to make the computation start
print "Blank lines: %d" % blanklines.value
Blank lines: 4

rdd.saveAsTextFile("output.txt") # this file will go to the the user's folder on hdfs
# this is happening because we are working within a hadoop ecosystem

# this will be saved in the normal file system (local), but in parts.
rdd.saveAsTextFile("file:///home/maria_dev/kamel/Spark_Training/chap6/outLocal.txt")
