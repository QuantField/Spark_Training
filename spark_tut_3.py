# Taking a sample from and RDD
rdd1 = sc.parallelize(list(range(10)))
rdd2 = rdd1.takeSample(True, 5, 233)
rdd2
#[7, 1, 3, 2, 9]
#True refers to withReplacement flag
# other examples 
rdd1.top(2)
rdd1.take(3)
rdd1.first()
