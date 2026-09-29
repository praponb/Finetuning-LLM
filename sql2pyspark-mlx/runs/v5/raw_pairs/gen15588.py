from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host").orderBy("status_code_high")
w4 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_first())
w5 = Window.partitionBy("service").orderBy(F.col("flag_bit8").desc_nulls_last())
w_recent = Window.partitionBy("service")
w_part = Window.partitionBy("service", "level").orderBy(F.col("status_code_high_percent_rank").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("host").isin(["h1", "h4"]))
    .select(
        "service",
        "host",
        "level",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    flattened_attrs
    .filter(F.col("service") == "checkout")
    .withColumn("level_avg_status_code_high", F.avg("status_code_high").over(w))
    .withColumn("host_avg_status_code_high", F.avg("status_code_high").over(w2))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w4))
    .withColumn("flag_bit8_percent_rank", F.percent_rank().over(w5))
)

cleaned = (
    enriched
    .filter(F.col("status_code_high") <= 2)
    .select(
        "service",
        "host",
        "level",
        "value",
        "status_code_high",
        "level_avg_status_code_high",
        "host_avg_status_code_high",
        "status_code_high_cume_dist",
        "status_code_high_percent_rank",
        "flag_bit8_percent_rank",
        (F.col("status_code_high") / F.nullif("status_code_high_cume_dist", F.lit(0))).alias("status_code_high_per_status_code_high_cume_dist"),
    )
)

remaining_keys = (
    cleaned
    .select("service", "level")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") > 1191)
        .select("service", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.sum("status_code_high_cume_dist").over(w_recent).alias("service_sum_status_code_high_cume_dist"),
        F.min("status_code_high_per_status_code_high_cume_dist").over(w).alias("level_min_status_code_high_per_status_code_high_cume_dist"),
        F.percent_rank().over(w_part).alias("status_code_high_percent_rank_percent_rank"),
    )
)

result = (
    with_windows
    .withColumn("level_avg_status_code_high_tier", F.when(F.col("level_avg_status_code_high") >= 2, "PREMIUM").otherwise("STANDARD"))
    .withColumn("service_sum_status_code_high_cume_dist_tier", F.when(F.col("service_sum_status_code_high_cume_dist") >= 4, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["level_avg_status_code_high_tier", "service_sum_status_code_high_cume_dist_tier", "service"], ["level_avg_status_code_high_tier", "service_sum_status_code_high_cume_dist_tier"], ["level_avg_status_code_high_tier"]],
        "level_avg_status_code_high_tier",
        "service_sum_status_code_high_cume_dist_tier",
        "service",
    )
    .agg(
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.round(F.avg("service_sum_status_code_high_cume_dist"), 2).alias("avg_service_sum_status_code_high_cume_dist"),
        F.round(F.avg("service_sum_status_code_high_cume_dist"), 2).alias("avg_service_sum_status_code_high_cume_dist_2"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.grouping("level_avg_status_code_high_tier").alias("grouping_level"),
    )
    .filter(F.col("avg_service_sum_status_code_high_cume_dist_2") < 1000000)
)
