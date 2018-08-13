# Pair RDD : Key/Value Pair transformation
# are defined as tuples
rdd1 = sc.parallelize([(1,2),(3,4),(3,6)])
rdd1.collect()
[(1, 2), (3, 4), (3, 6)]
rdd1.keys().collect()
[1, 3, 3]
rdd1.values().collect()
[2, 4, 6]

rdd2 = rdd1.reduceByKey(lambda x,y : x+y)
rdd2.collect()
#[(1, 2), (3, 10)]

rdd2 = rdd1.groupByKey()
# and this how to print the result
for s,t in rdd2.collect():
     print s,list(t)
1 [2]
3 [4, 6]

rdd3 = rdd1.mapValues(lambda x : x+1)
rdd3.collect()
[(1, 3), (3, 5), (3, 7)]

rdd4 = rdd1.flatMapValues(lambda x : range(x,6))
rdd4.collect()
[(1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5)]

# operations on two pair RDDs
rdd2 = sc.parallelize([(3,9)])
rdd3 = rdd1.subtractByKey(rdd2)
rdd3.collect()
[(1, 2)]

# inner join
>> rdd5 = rdd1.join(rdd2)
>>> rdd5.collect()
[(3, (4, 9)), (3, (6, 9))]

# left join
 rdd6 = rdd1.leftOuterJoin(rdd2)
>>> rdd6.collect()
[(1, (2, None)), (3, (4, 9)), (3, (6, 9))]

# right join
>> rdd7 = rdd1.rightOuterJoin(rdd2)
>>> rdd7.collect()
[(3, (4, 9)), (3, (6, 9))]

#group data from both rdds sharing the same key
rdd8 = rdd1.cogroup(rdd2)
>> rdd8.keys().collect()
[1, 3]
>> for r,s in rdd8.values().collect():
...     print list(r),list(s)
... 
[2] []
[4, 6] [9]


# filtering.. just act on the second element of the tuple
rdd = sc.parallelize([('holden','likes coffee'),('panda','likes long strings and coffee')])
>>> rdd.keys().collect()
['holden', 'panda']
>>> rdd.values().collect()
['likes coffee', 'likes long strings and coffee']
# filter for string length < 20
>>> rdd2 = rdd.filter(lambda x : len(x[1])<20)
>>> rdd2.collect()
[('holden', 'likes coffee')]
>>> 





