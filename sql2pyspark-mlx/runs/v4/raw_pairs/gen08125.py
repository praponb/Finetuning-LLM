from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").desc(), "id", "k")
w2 = Window.orderBy("status_code_high", "id", "k").rowsBetween(-6, -1)
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_first(), "id", "k")
w4 = Window.partitionBy("level").orderBy("latency_ms", "id", "k")
w5 = Window.partitionBy("level", "k")
w6 = Window.partitionBy("k").orderBy(F.col("status_code_high_avg_6_prior_per_info").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "status_code_high",
        F.col("status_code_high").cast("string").alias("status_code_high_num"),
        F.concat_ws("/", "k", "level").alias("k_level_key"),
    )
)

scored = (
    filtered
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("status_code_high_avg_6_prior", F.avg("status_code_high").over(w2))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

scored_app_logs = (
    ranked
    .select(
        "*",
        F.lead("status_code_high_avg_6_prior", 1).over(w4).alias("next_status_code_high_avg_6_prior"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4).alias("row_seq_share"),
    )
)

stats = (
    scored_app_logs
    .groupBy(
        "k",
        F.when(F.col("next_status_code_high_avg_6_prior") >= 3, "LARGE").otherwise("SMALL").alias("next_status_code_high_avg_6_prior_tier"),
    )
    .agg(
        F.round(F.sum("status_code_high_avg_6_prior") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("status_code_high_avg_6_prior_per_info"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("status_code_high_per_error"),
        F.sum("next_status_code_high_avg_6_prior").alias("total_next_status_code_high_avg_6_prior"),
        F.max(F.when(F.col("level") == "DEBUG", F.col("next_status_code_high_avg_6_prior"))).alias("debug_max_next_status_code_high_avg_6_prior"),
        F.round(F.sum("status_code_high_avg_6_prior") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("status_code_high_avg_6_prior_per_info_2"),
        F.sum("row_seq_share").alias("total_row_seq_share"),
    )
    .withColumn("status_code_high_avg_6_prior_per_info_percent_rank", F.percent_rank().over(w6))
)

result = (
    stats
    .select(
        "k",
        "next_status_code_high_avg_6_prior_tier",
        "status_code_high_per_error",
        "total_next_status_code_high_avg_6_prior",
        "debug_max_next_status_code_high_avg_6_prior",
        "status_code_high_avg_6_prior_per_info_2",
    )
    .orderBy(
        F.col("next_status_code_high_avg_6_prior_tier").desc(),
        F.col("status_code_high_avg_6_prior_per_info_2").desc(),
    )
)
