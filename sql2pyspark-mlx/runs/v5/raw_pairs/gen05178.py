from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("is_checkout_str").desc_nulls_first())
w2 = Window.orderBy(F.col("is_checkout_str").desc_nulls_last())
w3 = Window.partitionBy("host")
w4 = Window.orderBy(F.col("is_checkout_str_rank").desc_nulls_first())

flattened = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("k", "v"))
)

cleaned = (
    flattened
    .filter(F.col("service") == "auth")
    .select(
        "host",
        "k",
        F.when(F.col("service") == "checkout", 1).otherwise(0).alias("is_checkout"),
        F.lower("host").alias("host_lower"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

valid = (
    cleaned
    .filter(F.col("k").eqNullSafe(F.lit("version")))
    .select(
        "host",
        "k",
        "is_checkout",
        "k_filled",
        F.when(F.col("is_checkout") >= 76, "High").when(F.col("is_checkout") >= 33, "Medium").otherwise("Low").alias("is_checkout_tier"),
        F.when(F.col("k").isin(["trace", "version"]), "Group A").otherwise("Other").alias("k_group"),
    )
)

base = (
    valid
    .filter((~F.col("k").eqNullSafe(F.lit("trace"))) & (F.col("host") != "h1"))
    .select(
        "*",
        F.col("is_checkout").cast("double").alias("is_checkout_str"),
        F.upper("host").alias("host_upper"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("is_checkout_str_dense_rank"),
        F.rank().over(w2).alias("is_checkout_str_rank"),
        F.sum("is_checkout").over(w3).alias("host_sum_is_checkout"),
    )
)

valid_app_logs = (
    scored
    .filter(F.col("host") == "h4")
    .select(
        "host",
        "k",
        "k_filled",
        "is_checkout_tier",
        "k_group",
        "is_checkout_str",
        "is_checkout_str_dense_rank",
        "is_checkout_str_rank",
        "host_sum_is_checkout",
        F.least("host_sum_is_checkout", "is_checkout_str").alias("least_host_sum_is_checkout_is_checkout_str"),
        F.round("is_checkout_str_dense_rank", 1).alias("is_checkout_str_dense_rank_rounded"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled_2"),
    )
)

with_windows = (
    valid_app_logs
    .select("*", F.dense_rank().over(w4).alias("is_checkout_str_rank_dense_rank"))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("least_host_sum_is_checkout_is_checkout_str") >= 77, "PREMIUM").otherwise("STANDARD").alias("least_host_sum_is_checkout_is_checkout_str_tier"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.percentile_approx("is_checkout_str_dense_rank", 0.95).alias("is_checkout_str_dense_rank_p95"),
    )
    .orderBy("least_host_sum_is_checkout_is_checkout_str_tier")
)
