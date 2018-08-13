# persist in pyspark, scala is more straight-forward

sc.parallelize(list(range(10)))
rdd2 = rdd1.map(lambda x : x**2)
import pyspark
rdd2.persist(pyspak.StorageLevel.DISK_ONLY)
rdd2.getStorageLevel()
StorageLevel(True, False, False, False, 1)
# class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)
