from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service")
w2 = Window.partitionBy("host", "service").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        F.concat_ws(",", "level", "service").alias("level_service_key"),
    )
)

enriched = (
    filtered
    .filter(F.col("level").isin(["WARN", "DEBUG"]))
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
        F.sum("status_code").over(w3).alias("host_sum_status_code"),
        F.count("status_code").over(w4).alias("level_count_status_code"),
    )
)

base = (
    enriched
    .filter(F.col("host_sum_status_code") > 1114)
    .select(
        "*",
        (F.col("status_code_share") * F.col("status_code")).alias("status_code_share_times_status_code"),
    )
)

base_app_logs = (
    base
    .filter((~F.col("attr_key").eqNullSafe(F.lit("version"))) & (F.col("host") == "h2"))
    .select(
        "*",
        F.trim("host").alias("clean_host"),
        (F.col("status_code_share_times_status_code") * F.col("host_sum_status_code")).alias("status_code_share_times_status_code_times_host_sum_status_code"),
    )
)

cleaned = (
    base_app_logs
    .filter(F.col("status_code_share_times_status_code_times_host_sum_status_code").between(33, 68))
    .select(
        "id",
        "host",
        "status_code",
        "host_sum_status_code",
        "level_count_status_code",
        "status_code_share_times_status_code",
        "status_code_share_times_status_code_times_host_sum_status_code",
        F.round((F.col("level_count_status_code") * 1.15), 2).alias("level_count_status_code_scaled"),
        F.round((F.col("status_code_share_times_status_code_times_host_sum_status_code") / F.col("level_count_status_code")), 2).alias("status_code_share_times_status_code_times_host_sum_status_code_per_level_count_status_code"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("level_count_status_code_scaled") >= 68, "PREMIUM").otherwise("STANDARD").alias("level_count_status_code_scaled_tier"),
        "host",
        F.when(F.col("level_count_status_code") >= 16, "HIGH").otherwise("LOW").alias("level_count_status_code_tier"),
    )
    .agg(
        F.max(F.when(F.col("host") == "h4", F.col("level_count_status_code"))).alias("h4_max_level_count_status_code"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_rows"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("status_code_per_h4"),
    )
    .orderBy(F.col("h3_count").desc_nulls_first())
)
