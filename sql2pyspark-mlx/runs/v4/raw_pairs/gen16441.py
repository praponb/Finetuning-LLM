from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("log_ts", "id", "k").rowsBetween(-2, -1)
w3 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy(F.col("error_count").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_2_prior"),
        F.dense_rank().over(w3).alias("status_code_dense_rank"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_3"),
    )
)

base = (
    enriched
    .filter(F.col("k").isin(["trace", "region", "version"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "k",
        "attr_value",
        "first_known_latency_ms",
        "status_code_dense_rank",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w5).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w5) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("burst_id", F.sum("is_new_session").over(w6))

result = (
    sessions_app_logs
    .groupBy("level", F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
    )
    .withColumn("error_count_dense_rank", F.dense_rank().over(w7))
    .orderBy("level")
)
