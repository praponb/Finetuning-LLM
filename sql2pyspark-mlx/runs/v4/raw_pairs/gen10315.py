from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency")
w_grp = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_max_element").desc_nulls_last())
w5 = Window.orderBy(F.col("currency_stddev_samp_element_dense_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

metrics = (
    exploded
    .withColumn("element_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_max_element", F.max("element").over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.stddev_samp("element_dense_rank").over(w2).alias("currency_stddev_samp_element_dense_rank"),
        F.cume_dist().over(w_grp).alias("element_dense_rank_cume_dist"),
        F.max("currency_max_element").over(w2).alias("currency_max_currency_max_element"),
        F.count("element_dense_rank").over(w2).alias("currency_count_element_dense_rank"),
        F.cume_dist().over(w4).alias("currency_max_element_cume_dist"),
    )
)

cleaned = (
    enriched
    .filter(F.col("currency_max_element").isNotNull())
    .select(
        "*",
        (F.col("element") * F.col("currency_stddev_samp_element_dense_rank")).alias("element_times_currency_stddev_samp_element_dense_rank"),
    )
)

filtered = (
    cleaned
    .filter(F.col("element").isNotNull())
    .withColumn("element_dense_rank_cume_dist_adjusted", F.round((F.col("element_dense_rank_cume_dist") * 100), 2))
    .withColumn("greatest_currency_count_element_dense_rank_currency_max_currency_max_element", F.greatest("currency_count_element_dense_rank", "currency_max_currency_max_element"))
)

enriched_price_tiers = (
    filtered
    .withColumn("currency_avg_currency_max_currency_max_element", F.avg("currency_max_currency_max_element").over(w2))
    .withColumn("currency_stddev_samp_element_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("currency_min_currency_stddev_samp_element_dense_rank", F.min("currency_stddev_samp_element_dense_rank").over(w2))
)

grouped = (
    enriched_price_tiers
    .groupBy(
        F.when(F.col("element_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("element_dense_rank_tier"),
    )
    .agg(
        F.sum("currency_avg_currency_max_currency_max_element").alias("total_currency_avg_currency_max_currency_max_element"),
        F.percentile_approx("element", 0.9).alias("element_p90"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    grouped
    .select(
        "element_dense_rank_tier",
        "total_currency_avg_currency_max_currency_max_element",
        "num_rows",
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
