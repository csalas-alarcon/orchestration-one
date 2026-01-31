from pyspark.sql import SparkSession

def main():
   spark = SparkSession.builder.appName("Spark_Direct_Submit").getOrCreate()

   # Read data into a dataframe
   df = spark.read.csv("/data/data.csv",header="true")

   df.show()
   spark.stop()

if __name__ == "__main__":
   main()
