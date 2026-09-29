from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-03-18").cast("date")) & F.col("browser").isNotNull())
    .withColumn("started_at_hour", F.hour("started_at"))
)

result = (
    base
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("started_at_hour_tier", F.when(F.col("started_at_hour") >= 86, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "started_at_hour_tier"], ["week_start"], []],
        "week_start",
        "started_at_hour_tier",
    )
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.min("started_at").alias("first_seen"),
        F.round(F.avg("started_at_hour"), 2).alias("avg_started_at_hour"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("started_at_hour_tier")
)
