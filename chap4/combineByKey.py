
data = [
        ('A', 2.), ('A', 4.), ('A', 9.), 
        ('B', 10.), ('B', 20.), 
        ('Z', 3.), ('Z', 5.), ('Z', 8.), ('Z', 12.) 
       ]

rdd = sc.parallelize( data )

sumCount = rdd.combineByKey(lambda value: (value, 1),
                            lambda x, value: (x[0] + value, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1])
                           )
sumCount.collect()
[('A', (15.0, 3)), ('Z', (28.0, 4)), ('B', (30.0, 2))]

averageByKey = sumCount.map(lambda (key, (totalSum, count)): (key, totalSum / count))

averageByKey.collect()
[('A', 5.0), ('Z', 7.0), ('B', 15.0)]

# returns a dictionary
averageByKey.collectAsMap()
{'A': 5.0, 'B': 15.0, 'Z': 7.0}


# refer to https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/combine-by-key/spark-combineByKey.md
"""
lambda value: (value, 1) this is the data structure (sum, count), this is called a combiner

lambda x, value: (x[0] + value, x[1] + 1) this tells what to do when anew value is encountred sum + value, count+1.

lambda x, y: (x[0] + y[0], x[1] + y[1]) this is how to combine two combiner (merge two combiners)
                           
"""
