from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session():
    """
    Creates a Spark session.
    Delta support is pre-configured via Docker Env Vars.
    """
    return (
        SparkSession.builder.master("local[2]")  # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .appName("DevContainerDemo")
        .getOrCreate()
    )


def process_revenue(df):
    """
    Standardizes revenue data:
    1. Filters non-active accounts.
    2. Calculates tax.
    """
    return df.filter(F.col("status") == "active").withColumn(
        "total_revenue", F.col("amount") * 1.2
    )


if __name__ == "__main__":
    print("Starting ETL process...fooooooo baaar baaz")
    spark = get_spark_session()

    print("\nwelcome to debian based pipelines your interactive spark session is ready to use\n")
    # Mock Data Creation
    data = [("abcd", "active", 3000.0), ("Client_B", "inactive", 500.0)]
    df = spark.createDataFrame(data, ["client", "status", "amount"])

    # Transformation
    result = process_revenue(df)

    # Write to Delta Lake
    result.write.format("delta").mode("overwrite").save("./revenue_data2")
    result.show()
    print("ETL process completed successfully!!!!! foooo baaaaaaz", flush=True)