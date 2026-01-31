from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date


def main():
    spark = (
        SparkSession.builder
        .appName("iceberg-test")  # lo dejo igual que en el notebook
        .getOrCreate()
    )

    # 1) Leer CSV desde MinIO (S3A)
    df = spark.read.option("header", "true").csv("s3a://bronze/flights/departuredelays.csv")
    df.show(5, truncate=False)

    # 2) (en el notebook generas df_silver, aunque luego guardas df)
    df_silver = df.withColumn("ingestion_date", current_date())

    # 3) Crear DB silver. En iceber se llaman Namespaces
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("SHOW NAMESPACES IN iceberg").show(truncate=False)
    spark.sql("SHOW TABLES IN iceberg.silver").show()

    # 4) Guardar tabla Iceberg (igual que notebook: guardas df, no df_silver)
    #En la versión actual de Iceberg no se puede usar la misma sintaxis que con Delta. La siguiente sentencia no funcionará bien
    # df_silver.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.silver.flights")

    df_silver.writeTo("iceberg.silver.flights").using("iceberg").createOrReplace()


    # 5) Ver tablas y leer la tabla Iceberg
    spark.sql("SHOW TABLES IN iceberg.silver").show()
    df_from_iceberg = (
        spark.read
        .format("iceberg")
        .table("iceberg.silver.flights")
    )
    df_from_iceberg.show()

    spark.stop()


if __name__ == "__main__":
    main()
