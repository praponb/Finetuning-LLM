from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").desc(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

with_prev = lines.withColumn("prev_log_ts", F.lag("log_ts").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

filtered = (
    sessionized
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .rollup(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "key")
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("island_id_per_error"),
        F.count_if(F.col("key") == "version").alias("version_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("latency_ms_per_error"),
    )
)
