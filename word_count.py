

rdd  = sc.textFile("file:/home/kamel_dev/randText.txt")

def clean(line):
    s = line.replace('.',' ').replace(',',' ').replace('?',' ')
    return s.lower()

rdd2 = rdd.map(clean).flatMap(lambda x : x.split())
rdd3 = rdd2.map(lambda x:(x,1))  # create Pair RDD
#--- count the word frequency 


