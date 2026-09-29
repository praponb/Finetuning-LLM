from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy("island_id_2", "id", "attr_key").rowsBetween(-6, -1)
w7 = Window.orderBy("session_start_flag", "id", "attr_key")
w8 = Window.partitionBy("attr_key", "host").orderBy("session_no", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

with_gaps = (
    flattened
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

flagged = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w3).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w3) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("session_no", F.sum("is_new_burst").over(w4))

filtered = (
    sessions_app_logs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "island_id",
        "session_no",
        F.lower("host").alias("host_lower"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

flagged_2 = (
    ranked
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w3).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = flagged_2.withColumn("island_id_2", F.sum("session_start_flag").over(w4))

metrics = (
    islands
    .select(
        "*",
        F.count("island_id_2").over(w5).alias("island_id_2_count_15m"),
        F.avg("status_code").over(w6).alias("status_code_avg_6_prior"),
        F.sum("island_id_2").over(w5).alias("island_id_2_sum_15m"),
        F.ntile(5).over(w7).alias("session_start_flag_bucket"),
        F.lag("session_start_flag", 2).over(w8).alias("prev_session_start_flag"),
    )
)

result = (
    metrics
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .rollup("island_id", "hour_window")
    .agg(
        F.max(F.when(F.col("host") == "h1", F.col("island_id_2_sum_15m"))).alias("h1_max_island_id_2_sum_15m"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.grouping("island_id").alias("grouping_id"),
    )
)
