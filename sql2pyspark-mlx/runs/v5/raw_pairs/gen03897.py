from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("k", "level").orderBy("session_no", "id", "k").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("level").orderBy(F.col("log_ts_hour").desc_nulls_last(), "id", "k")
w6 = Window.partitionBy("level").orderBy(F.col("last7_count_is_new_session").asc_nulls_first())
w7 = Window.partitionBy("k", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.orderBy("latency_ms_share", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

gaps = (
    exploded
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

base = (
    sessionized
    .filter(F.col("is_new_session") != 0)
    .withColumn("is_new_session_filled", F.coalesce("is_new_session", F.lit(0)))
    .withColumn("k_filled", F.coalesce("k", F.lit("N/A")))
)

metrics = (
    base
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        "attr_value",
        "k_filled",
        F.sum("is_new_session").over(w_part).alias("last7_sum_is_new_session"),
        F.count("is_new_session").over(w_part).alias("last7_count_is_new_session"),
        F.avg("is_new_session").over(w_part).alias("last7_avg_is_new_session"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("log_ts") >= "2023-08-01")
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        "attr_value",
        "k_filled",
        "last7_sum_is_new_session",
        "last7_count_is_new_session",
        "last7_avg_is_new_session",
        "latency_ms_share",
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .filter(F.col("last7_avg_is_new_session").between(0.22, 0.58))
    .select(
        "id",
        "level",
        "k",
        "k_filled",
        "last7_count_is_new_session",
        "latency_ms_share",
        "log_ts_hour",
        "rn",
        F.dense_rank().over(w6).alias("last7_count_is_new_session_dense_rank"),
        F.count("log_ts_hour").over(w7).alias("log_ts_hour_count_15m"),
        F.last("latency_ms_share", ignorenulls=True).over(w8).alias("last_known_latency_ms_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "level",
        "k",
        "last7_count_is_new_session",
        "log_ts_hour",
        "rn",
        "last7_count_is_new_session_dense_rank",
        "log_ts_hour_count_15m",
        "last_known_latency_ms_share",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "GROUP_A").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("id").desc(), "last7_count_is_new_session")
)
