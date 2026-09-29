from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("latency_ms_tier").orderBy(F.col("latency_ms_per_region").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("latency_ms") >= 2022, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        "key",
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "ERROR", F.col("latency_ms")).otherwise(0)).alias("error_latency_ms"),
        F.count_if(F.col("level") == "INFO").alias("info_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("key") == "region", 1)), F.lit(0)), 2).alias("latency_ms_per_region"),
    )
    .withColumn("latency_ms_per_region_rank", F.rank().over(w))
    .orderBy(F.col("key").desc())
)
