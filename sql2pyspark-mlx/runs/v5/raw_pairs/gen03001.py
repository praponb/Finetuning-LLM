from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-28 20:00:00").cast("timestamp")) & (F.col("customer_id") == 19))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.upper("browser").alias("browser_upper"),
    )
)

result = (
    filtered
    .withColumn("activity_month", F.date_trunc("month", "started_at"))
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .rollup("activity_month", "country", "week_start")
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count("*").alias("row_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("uk_events") > 1)
    .orderBy(F.col("uk_events").desc())
)
