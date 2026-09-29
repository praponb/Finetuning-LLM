from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

gaps = (
    lines
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

matched_keys = (
    sessionized
    .select("host", "service")
    .intersect(
        df_app_logs
        .filter(F.col("host") == "h2")
        .select("host", "service")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 07:00:00"))) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        F.hour("log_ts").alias("log_ts_hour"),
        F.date_trunc("WEEK", "log_ts").alias("log_ts_period"),
    )
)

result = (
    valid
    .groupBy("service", F.date_trunc("WEEK", "log_ts_period").alias("week_start"))
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .orderBy(F.col("version_events").desc_nulls_last())
)
