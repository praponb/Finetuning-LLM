from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k").orderBy("status_code", "id", "k")
w_ordered = Window.partitionBy("service")
w3 = Window.partitionBy("level").orderBy("corr_latency_ms_status_code_per_status_code", "id", "k")
w4 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "k",
        F.lead("status_code", 2).over(w_running).alias("next_status_code"),
        F.corr("latency_ms", "status_code").over(w_ordered).alias("corr_latency_ms_status_code"),
    )
)

valid = (
    metrics
    .filter(F.col("service") != "auth")
    .withColumn("corr_latency_ms_status_code_per_status_code", F.round((F.col("corr_latency_ms_status_code") / F.col("status_code")), 2))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "level", "status_code", "k")
)

cleaned = (
    latest
    .filter(F.col("k") == "region")
    .select(
        "*",
        F.initcap(F.trim("level")).alias("level_title"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "level")
    .intersect(
        df_app_logs
        .filter(F.col("service") == "search")
        .select("id", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics_app_logs = (
    reconciled
    .select(
        "level",
        "status_code",
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
    )
)

result = (
    metrics_app_logs
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code_percent_rank")).otherwise(0)).alias("info_status_code_percent_rank"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.percentile_approx("status_code_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_percent_rank_quartiles"),
    )
    .filter(F.col("status_code_bit_and") < 1000)
    .orderBy(F.col("status_code_bit_and").desc())
)
