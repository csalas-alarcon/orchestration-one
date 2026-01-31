import sys
import great_expectations as gx
from pyspark.sql import SparkSession

# 1) Proyecto GE existente (se montará desde Airflow a /opt/gx)
GE_PROJECT_ROOT = "/opt/gx"

# 2) Suite ya creada en Jupyter
SUITE_NAME = "bronze_vuelos_suite"

# 3) Datos a validar
BRONZE_PATH = "s3a://bronze/flights/departuredelays.csv"

def main() -> int:
    spark = SparkSession.builder.getOrCreate()

    # Leer datos
    df = spark.read.option("header", "true").csv(BRONZE_PATH)

    # Cargar proyecto GE EXISTENTE
    context = gx.get_context(project_root_dir=GE_PROJECT_ROOT)

    # Crear batch desde DF Spark (en memoria)
    source = context.sources.add_or_update_spark(name="spark")
    asset = source.add_dataframe_asset(name="departuredelays_df")
    batch_request = asset.build_batch_request(dataframe=df)

    # Reutilizar la suite existente y validar
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME
    )

    results = validator.validate()
    success = bool(results.get("success", False))

    print("GE SUCCESS:", success)
    print("GE STATS:", results.get("statistics", {}))

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
