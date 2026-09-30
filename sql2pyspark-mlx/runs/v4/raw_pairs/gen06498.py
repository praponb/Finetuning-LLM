from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("num_levels").desc())

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("status_code") >= 401, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .withColumn("num_levels_rank", F.rank().over(w))
    .orderBy(F.col("warn_events").desc())
)
