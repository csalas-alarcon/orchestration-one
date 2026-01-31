from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date


def main():
    spark = (
        SparkSession.builder
        .appName("delta-test")  # lo dejo igual que en el notebook
        .getOrCreate()
    )

    # 1) Leer CSV desde MinIO (S3A)
    df = spark.read.option("header", "true").csv("s3a://bronze/flights/")
    df.show(5, truncate=False)

    # 2) (en el notebook generas df_silver, aunque luego guardas df)
    df_silver = df.withColumn("ingestion_date", current_date())

    # 3) Crear DB silver
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("SHOW DATABASES").show(truncate=False)
    spark.sql("SHOW TABLES IN silver").show(truncate=False)

    # 4) Guardar tabla Delta (igual que notebook: guardas df, no df_silver)
    df_silver.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.flights")

    # 5) Ver tablas y leer la tabla Delta
    spark.sql("SHOW TABLES IN silver").show(truncate=False)

    df_from_delta = (
        spark.read
        .format("delta")
        .table("silver.flights")
    )
    df_from_delta.show()

    spark.stop()


if __name__ == "__main__":
    main()
