from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency")
w_recent = Window.partitionBy("currency").orderBy(F.col("currency_sum_element").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_sum_currency_max_element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "currency",
        "element",
        F.rank().over(w).alias("element_rank"),
        F.sum("element").over(w2).alias("currency_sum_element"),
    )
)

filtered = (
    metrics
    .filter(F.col("element_rank") == 4)
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

metrics_price_tiers = (
    filtered
    .select(
        "*",
        F.max("element").over(w2).alias("currency_max_element"),
        F.stddev_samp("element").over(w2).alias("currency_stddev_samp_element"),
        F.percent_rank().over(w_recent).alias("currency_sum_element_percent_rank"),
        F.sum("currency_sum_element").over(w2).alias("currency_sum_currency_sum_element"),
        F.min("currency_sum_element").over(w2).alias("currency_min_currency_sum_element"),
    )
)

with_windows = (
    metrics_price_tiers
    .select(
        "id",
        "currency",
        "element",
        "element_rank",
        "currency_sum_element",
        "currency_title",
        "currency_stddev_samp_element",
        "currency_sum_element_percent_rank",
        "currency_sum_currency_sum_element",
        "currency_min_currency_sum_element",
        F.sum("currency_max_element").over(w2).alias("currency_sum_currency_max_element"),
        F.avg("currency_sum_element").over(w2).alias("currency_avg_currency_sum_element"),
    )
)

valid = (
    with_windows
    .filter(F.col("currency_sum_element") <= 150)
    .select(
        "*",
        F.round((F.col("currency_sum_element_percent_rank") * 0.9), 2).alias("currency_sum_element_percent_rank_scaled"),
    )
)

result = (
    valid
    .select(
        "currency_title",
        "currency_sum_currency_sum_element",
        "currency_min_currency_sum_element",
        F.percent_rank().over(w4).alias("currency_sum_currency_max_element_percent_rank"),
        F.count("currency_sum_currency_sum_element").over(w2).alias("currency_count_currency_sum_currency_sum_element"),
    )
)
