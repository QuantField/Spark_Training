# Creating Pair RDD
# Here the key is the first word of the line
rdd1 = sc.textFile("../sampletext.txt")
rdd1.count()
# 6
rdd2 = rdd1.map(lambda line : (line.split()[0], line))
rdd2.first()
#(u'This', u'This is a first line of the text.')
# in scala, 
#val pairs = rdd1.map(x => (x.split(" ")(0), x))
