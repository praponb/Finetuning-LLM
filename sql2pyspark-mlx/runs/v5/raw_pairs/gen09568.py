from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "started_at",
        "country",
        "customer_id",
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)
