from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .filter(F.col("london_count") < 5)
    .drop("london_count")
    .orderBy(F.col("cnt").desc())
)
