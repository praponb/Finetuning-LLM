from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-07-01")
    .select(
        "id",
        "country",
        "browser",
        "customer_id",
        F.substring("browser", 1, 3).alias("browser_prefix"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)
