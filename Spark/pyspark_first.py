from pyspark import SparkContext, SparkConf
from operator import add
conf = SparkConf().setAppName("First App").setMaster("local")
sc = SparkContext(conf=conf)
# sc = SparkContext("local", "First App")
sc.setLogLevel("WARN")  # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
print("---------------------------------------------------------------------------------\n\r")

# find number of a's & b's in a file
logFile = "file:///C:/Users/adam l/Desktop/main.cpp"
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

# count number of words in a list (saved in the RDD)
words = sc.parallelize(
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)

print("Number of elements in RDD -> %i" % words.count())

# get all the elements stored in the RDD
print("Elements in RDD -> %s" % words.collect())

# prints all the elements in the RDD
def f(x): print(x)
fore = words.foreach(f)

# filter out the strings containing ''spark"
print("Fitered RDD -> %s" % words.filter(lambda x: 'spark' in x).collect())

# map
print("Key value pair -> %s" % words.map(lambda x: (x, 1)).collect())

# reduce
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print("Adding all the elements -> %i" % adding)

# join
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print("Join RDD -> %s" % final)

# cache
words.cache()
print("Words got cached > %s" % words.persist().is_cached)

# broadcast var
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
print("Stored data -> %s" % words_new.value)
print("Printing a particular element in RDD -> %s" % words_new.value[2] )

# accumulator var
num = sc.accumulator(10)
def f(x):
   global num
   num+=x
rdd = sc.parallelize([20,30,40,50])
rdd.foreach(f)
final = num.value
print("Accumulated value is -> %i" % final)

