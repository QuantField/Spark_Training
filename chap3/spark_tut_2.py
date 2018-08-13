# using flatMap
rdd1 = sc.parallelize(["Hello World","Hi"])
rdd2 = rdd1.flatMap(lambda x : x.split())
rdd2.first()
# 'Hello'
