from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()
print("Spark OK:", spark.version)
spark.range(10).show()
spark.stop()
