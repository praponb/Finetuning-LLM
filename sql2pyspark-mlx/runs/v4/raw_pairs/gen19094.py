from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.device"), F.col("p.browser"))
)

result = (
    with_dims
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 470, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .rollup("duration_sec_tier", "week_start", "device")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum(F.when(F.col("browser") == "Safari", F.col("duration_sec")).otherwise(0)).alias("safari_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.grouping("duration_sec_tier").alias("gid"),
    )
    .orderBy(F.col("duration_sec_tier").asc_nulls_last())
)
