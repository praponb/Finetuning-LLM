from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 6)
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.upper("country").alias("country_upper"),
        F.dayofmonth("started_at").alias("started_at_day"),
    )
)
