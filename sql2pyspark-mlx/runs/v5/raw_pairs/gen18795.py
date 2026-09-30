from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & (F.col("started_at") >= "2024-04-03"))
    .select(
        "id",
        "customer_id",
        "started_at",
        F.initcap(F.trim("country")).alias("country_title"),
        F.trim("device").alias("clean_device"),
    )
)
