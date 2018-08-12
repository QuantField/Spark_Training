# RDD union, is not the same as set union as it keeps duplicates
rdd1 = sc.parallelize([3,4,20,3,7,8])
rdd2 = sc.parallelize([7,23,49,6,6,8])
rdd3 = rdd1.union(rdd2)
rdd3.collect()
#[3, 4, 20, 3, 7, 8, 7, 23, 49, 6, 6, 8]

# RDD intersection
rdd4 = rdd1.intersection(rdd2)
rdd4.collect()
#[8, 7]
