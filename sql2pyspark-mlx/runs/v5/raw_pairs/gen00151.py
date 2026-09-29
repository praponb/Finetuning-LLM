from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code")
w2 = Window.partitionBy("k")
w3 = Window.orderBy(F.col("status_code_rank").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "host",
        "latency_ms",
        "status_code",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("k").isNotNull())
    .select(
        "latency_ms",
        "status_code",
        "k",
        "value",
        "flag_bit2",
        "status_code_high",
        (F.col("latency_ms") * F.col("flag_bit2")).alias("latency_ms_times_flag_bit2"),
        F.when(F.col("latency_ms") >= 2616, "High").when(F.col("latency_ms") >= 1158, "Medium").otherwise("Low").alias("latency_ms_band"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
    )
)

enriched = (
    scored
    .filter(F.col("flag_bit2") >= 0)
    .select(
        "k",
        "value",
        "status_code_high",
        "latency_ms_times_flag_bit2",
        "status_code_rank",
        "corr_status_code_latency_ms",
        F.sum("status_code_high").over(w2).alias("k_sum_status_code_high"),
        F.rank().over(w3).alias("status_code_rank_rank"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
    )
)

base = (
    enriched
    .filter(F.col("status_code_rank_rank") >= 5)
    .select(
        "*",
        (F.col("status_code_high") * F.col("status_code_rank_rank")).alias("status_code_high_times_status_code_rank_rank"),
        F.when(F.col("status_code_rank") >= 8, "High").when(F.col("status_code_rank") >= 4, "Medium").otherwise("Low").alias("status_code_rank_band"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

result = (
    base
    .withColumn("status_code_rank_rank_tier", F.when(F.col("status_code_rank_rank") >= 9, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["k", "status_code_rank_rank_tier"], ["k"]],
        "k",
        "status_code_rank_rank_tier",
    )
    .agg(
        F.sum(F.when(F.col("k") == "version", F.col("corr_latency_ms_status_code")).otherwise(0)).alias("version_corr_latency_ms_status_code"),
        F.countDistinct("k").alias("distinct_ks"),
    )
    .orderBy(F.col("distinct_ks").desc_nulls_first())
)
