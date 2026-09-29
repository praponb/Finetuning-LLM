from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-07 03:00:00").cast("timestamp")) & ~F.col("country").isin(["SG", "US", "TH"]))
    .select(
        "id",
        "started_at",
        F.substring("browser", 1, 4).alias("browser_prefix"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.min("started_at").alias("started_at"),
        F.count("*").alias("event_count"),
    )
)
