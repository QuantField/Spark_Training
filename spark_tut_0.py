# Two ways of creating RDD
# Loading from a file
rdd1 = sc.textFile("lorum.txt")
rdd2 = rdd1.filter(lambda x : 'lorum' in x.lower())
rdd2.first() 

# From object, collections for instance with sc.parallelize
rdd1 = sc.parallelize(list(range(10)))
rdd2 = rdd1.map(lambda x : x**2)
rdd3 = rdd2.collect()
