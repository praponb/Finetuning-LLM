from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("level").orderBy("burst_id", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k", "level").orderBy(F.col("status_code_high").desc(), "id", "k")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("level", "k").orderBy(F.col("running_max_status_code").desc(), "id", "k")
w_part = Window.orderBy(F.col("running_avg_status_code").asc_nulls_last())
w8 = Window.partitionBy("level", "k").orderBy("running_sum_status_code")
w_recent = Window.partitionBy("level", "k")
w10 = Window.partitionBy("level").orderBy("event_seq", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("burst_id_avg_30m", "id", "k")
w12 = Window.partitionBy("k").orderBy("running_sum_status_code_cume_dist", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

flagged = (
    parsed
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

enriched = (
    islands
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        "status_code_high",
        "burst_id",
        F.sum("status_code").over(w_running).alias("running_sum_status_code"),
        F.max("status_code").over(w_running).alias("running_max_status_code"),
        F.avg("status_code").over(w_running).alias("running_avg_status_code"),
    )
)

enriched_app_logs = (
    enriched
    .filter(F.col("status_code_high") <= 2)
    .withColumn("status_code_high_bucket", F.ntile(5).over(w4))
    .withColumn("burst_id_avg_30m", F.avg("burst_id").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("running_avg_status_code_percent_rank", F.percent_rank().over(w_part))
    .withColumn("running_sum_status_code_cume_dist", F.cume_dist().over(w8))
)

metrics = (
    enriched_app_logs
    .withColumn("level_count_status_code", F.count("status_code").over(w_recent))
    .withColumn("running_status_code", F.sum("status_code").over(w10))
    .withColumn("prev_running_sum_status_code", F.lag("running_sum_status_code", 1).over(w11))
    .withColumn("first_known_running_avg_status_code", F.first("running_avg_status_code", ignorenulls=True).over(w12))
)

result = (
    metrics
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.countDistinct("k").alias("distinct_ks"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
    )
)
