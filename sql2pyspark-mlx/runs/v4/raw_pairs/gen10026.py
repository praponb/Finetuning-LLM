from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("is_h3").asc_nulls_first())
w_grp = Window.orderBy(F.col("is_h3").desc_nulls_last())
w_part = Window.partitionBy("key")
w4 = Window.orderBy(F.col("is_h3").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

base = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("level") == "ERROR"))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "key",
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled_2"),
    )
)

matched_keys = (
    base
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(~F.col("service").isin(["checkout", "search"]))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("latency_ms") >= 725)
    .select(
        "id",
        "host",
        "key",
        F.when(F.col("host") == "h3", 1).otherwise(0).alias("is_h3"),
        F.initcap(F.trim("level")).alias("level_title"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w_hist).alias("is_h3_percent_rank"),
        F.rank().over(w_grp).alias("is_h3_rank"),
    )
)

base_app_logs = (
    with_windows
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        (F.col("is_h3_rank") / F.nullif("is_h3_percent_rank", F.lit(0))).alias("is_h3_rank_per_is_h3_percent_rank"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

with_windows_app_logs = (
    base_app_logs
    .select(
        "*",
        F.sum("is_h3_rank").over(w_part).alias("key_sum_is_h3_rank"),
        F.percent_rank().over(w4).alias("is_h3_percent_rank_2"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "host",
        "key",
        "is_h3",
        "resolved_key",
        "is_h3_percent_rank",
        "is_h3_rank",
        "is_h3_rank_per_is_h3_percent_rank",
        "key_sum_is_h3_rank",
        F.when(F.col("host").rlike("^(h1|h3)$"), "EXTENDED").when(F.col("host").rlike("^(h2|h4)$"), "PRIORITY").otherwise("GROUP_A").alias("host_zone"),
    )
    .orderBy(F.col("id").desc())
)
