from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id", "attr_key")
w3 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("log_ts_hour").desc_nulls_first())
w5 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w_ordered = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("host", "attr_key")
w8 = Window.partitionBy("attr_key").orderBy("log_ts_hour_count_15m", "id", "attr_key").rowsBetween(-3, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .select("*", F.hour("log_ts").alias("log_ts_hour"))
)

enriched = (
    valid
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_key",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.row_number().over(w2).alias("event_seq"),
        F.count("log_ts_hour").over(w3).alias("log_ts_hour_count_15m"),
        F.percent_rank().over(w4).alias("log_ts_hour_percent_rank"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "event_seq",
        "log_ts_hour_percent_rank",
        F.sum("latency_ms").over(w_ordered).alias("running_latency_ms"),
        F.max("log_ts_hour_percent_rank").over(w7).alias("host_max_log_ts_hour_percent_rank"),
        F.avg("log_ts_hour_count_15m").over(w8).alias("log_ts_hour_count_15m_avg_3_prior"),
    )
)
