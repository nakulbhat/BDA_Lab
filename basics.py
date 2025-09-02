# Some basics about pyspark.

from pyspark.sql import SparkSession
from pyspark import SparkContext
spark = SparkSession.builder.appName("basics").getOrCreate()
sc: SparkContext = spark.sparkContext 

nums = range(1,11)
nums_rdd = sc.parallelize(nums)

# foreach just executes a function
nums_rdd.foreach(lambda x: print(x)) 

# map is a transformation and returns an rdd with elems from the application of the func
nums_squared = nums_rdd.map(lambda x: x * x)  
nums_squared.collect()

nums_rdd.max()
nums_rdd.mean()
