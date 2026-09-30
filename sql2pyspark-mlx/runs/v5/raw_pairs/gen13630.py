from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

flagged = (
    flattened
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        "value",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "log_ts", "latency_ms", "key")
)

remaining_keys = (
    latest
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms").isNotNull())
        .select("id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "log_ts",
        "latency_ms",
        "key",
        F.when(F.col("key").rlike("^(version|trace)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy("log_ts")
)
