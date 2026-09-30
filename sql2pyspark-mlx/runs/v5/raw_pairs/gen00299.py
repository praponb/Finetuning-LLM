from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2024-03-01") & F.col("customer_id").isin([4, 24]))
    .select(
        "id",
        "country",
        F.lower("country").alias("country_lower"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)
