from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key")
w2 = Window.partitionBy("key", "level")
w3 = Window.partitionBy("level").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    flattened
    .filter(F.col("level") == "ERROR")
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    parsed
    .withColumn("level_avg_status_code", F.avg("status_code").over(w))
    .withColumn("key_max_status_code", F.max("status_code").over(w2))
    .withColumn("running_is_flagged", F.sum("is_flagged").over(w3))
)

enriched = (
    with_windows
    .select(
        "*",
        F.round((F.col("level_avg_status_code") / F.sum("level_avg_status_code").over(w)), 4).alias("level_avg_status_code_share"),
        F.sum("key_max_status_code").over(w4).alias("cumulative_key_max_status_code"),
    )
)

matched_keys = (
    enriched
    .select("level", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("service").isin(["billing", "auth", "search"]))
        .select("level", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("level", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("level", "id")
    )
)

verified_app_logs = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

pivoted = (
    verified_app_logs
    .groupBy("level")
    .pivot("key", ["version", "region", "trace"])
    .agg(F.avg("is_flagged").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    pivoted
    .select("level", "version_total", "version_cnt", "trace_cnt")
    .orderBy("level")
)
