from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark import SparkContext
spark = SparkSession.builder.appName("basics").getOrCreate()
sc: SparkContext = spark.sparkContext 

df = spark.read.csv('./customers-100.csv', header=True, inferSchema=True)
df.first()

df.schema

# df.select also returns a df. and needs to call show to work.
df.select(sf.mean(sf.col('Index'))).show()
