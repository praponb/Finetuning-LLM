from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("duration_min") > 0)
)
