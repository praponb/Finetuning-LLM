from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("key")
w5 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "key",
        "v",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.max("status_code").over(w2).alias("status_code_max_2h"),
        F.sum("status_code").over(w3).alias("status_code_sum_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
        F.max("status_code").over(w5).alias("status_code_max_30m"),
    )
)

base = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "service",
        "log_ts",
        "status_code_share",
        "status_code_max_2h",
        F.lower("key").alias("key_lower"),
    )
)

matched_keys = (
    base
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "DEBUG"]))
        .select("service")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("status_code_share") >= 0.74, "LARGE").otherwise("SMALL").alias("status_code_share_tier"),
        F.when(F.col("status_code_max_2h") >= 382, "LARGE").otherwise("SMALL").alias("status_code_max_2h_tier"),
    )
    .agg(
        F.round(F.avg("status_code_max_2h"), 2).alias("avg_status_code_max_2h"),
        F.max("log_ts").alias("last_seen"),
        F.round(F.avg("status_code_share"), 2).alias("avg_status_code_share"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.sort_array(F.collect_set("service")).alias("service_list_2"),
    )
    .orderBy("avg_status_code_share")
)
