from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_debug")
w2 = Window.partitionBy("host", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("host", "level")
w5 = Window.partitionBy("level", "host").orderBy(F.col("is_debug").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "attr_value",
        F.when(F.col("level") == "DEBUG", 1).otherwise(0).alias("is_debug"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

metrics = (
    cleaned
    .withColumn("is_debug_percent_rank", F.percent_rank().over(w))
    .withColumn("is_debug_max_15m", F.max("is_debug").over(w2))
    .withColumn("is_debug_avg_2h", F.avg("is_debug").over(w_running))
    .withColumn("host_count_is_debug", F.count("is_debug").over(w4))
    .withColumn("is_debug_dense_rank", F.dense_rank().over(w5))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("day", "log_ts").alias("activity_day"),
        F.date_trunc("hour", "log_ts").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.min("log_ts").alias("first_seen"),
        F.round(F.avg("is_debug_max_15m"), 2).alias("avg_is_debug_max_15m"),
        F.sum(F.when(F.col("host") == "h3", F.col("is_debug"))).alias("h3_total_is_debug"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
    )
    .filter(F.col("avg_is_debug_max_15m") < 0)
)
