from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"], []],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("started_at").alias("last_seen"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("event_count").desc())
)
