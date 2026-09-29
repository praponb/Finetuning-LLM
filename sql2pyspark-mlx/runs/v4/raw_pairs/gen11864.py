from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

with_prev = flattened.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

base = (
    sessionized
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull() & F.col("attr_key").isin(["region", "version"]))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "prev_log_ts",
        "is_new_session",
        F.round((F.col("is_new_session") * 100), 2).alias("is_new_session_scaled"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean"),
    )
)

remaining_keys = (
    base
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") < 1486)
        .select("host", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "attr_key",
        "prev_log_ts",
        "is_new_session_scaled",
    )
)
