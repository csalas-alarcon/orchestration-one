from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read-minio").getOrCreate()

path = "s3a://bronze/flights/departuredelays.csv"
df = spark.read.option("header", "true").csv(path)

df.show(5, truncate=False)
print("rows:", df.count())

spark.stop()
