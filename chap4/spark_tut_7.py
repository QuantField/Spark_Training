# Aggregations
# -- Average
rdd = sc.parallelize([('panda',0),('pink',3),('pirate',3),('panda',1),('pink',4)])

rdd2 = rdd.mapValues(lambda x : (x,1))
rdd.collect()
[('panda', 0), ('pink', 3), ('pirate', 3), ('panda', 1), ('pink', 4)]

rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd3.collect()
[('pink', (7, 2)), ('panda', (1, 2)), ('pirate', (3, 1))]
# average per key
rdd4 = rdd3.mapValues(lambda x : float(x[0])/float(x[1]))
rdd4.collect()
[('pink', 3.5), ('panda', 0.5), ('pirate', 3.0)]

# Word count, word and the number of occurence in the text
rdd1 = sc.textFile("../lorum.txt")
rdd2 = rdd1.flatMap(lambda x : x.split())
rdd3 = rdd2.map(lambda w : (w,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
#----Faster wayu=
rdd2b = rdd2.countByValue() # returns a dictionary


