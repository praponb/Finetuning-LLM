from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")
w2 = Window.partitionBy("currency")
w_grp = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w_hist = Window.partitionBy("currency").orderBy(F.col("element_rank").desc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank_2").desc_nulls_first())
w8 = Window.partitionBy("currency").orderBy("currency_sum_element")
w9 = Window.partitionBy("currency").orderBy(F.col("currency_avg_element").desc())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("element"))
)

scored = (
    flattened
    .withColumn("element_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_avg_element", F.avg("element").over(w2))
    .withColumn("element_rank", F.rank().over(w_grp))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w4))
)

metrics = (
    scored
    .select(
        "*",
        F.rank().over(w_hist).alias("element_rank_rank"),
        F.round((F.col("element_dense_rank_2") / F.sum("element_dense_rank_2").over(w2)), 4).alias("element_dense_rank_2_share"),
    )
)

matched_keys = (
    metrics
    .select("currency")
    .intersect(
        df_price_tiers
        .filter(F.col("plan").isin(["basic", "enterprise", "pro"]))
        .select("currency")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored_price_tiers = (
    kept_rows
    .withColumn("element_dense_rank_3", F.dense_rank().over(w6))
    .withColumn("currency_stddev_samp_element_dense_rank_2", F.stddev_samp("element_dense_rank_2").over(w2))
    .withColumn("currency_sum_element", F.sum("element").over(w2))
    .withColumn("element_dense_rank_2_rank", F.rank().over(w7))
)

scored_price_tiers_2 = (
    scored_price_tiers
    .select(
        "*",
        F.max("element_dense_rank_2_share").over(w2).alias("currency_max_element_dense_rank_2_share"),
        F.max("element_dense_rank_3").over(w2).alias("currency_max_element_dense_rank_3"),
        F.count("element").over(w2).alias("currency_count_element"),
        F.cume_dist().over(w8).alias("currency_sum_element_cume_dist"),
        F.rank().over(w9).alias("currency_avg_element_rank"),
    )
)

base = (
    scored_price_tiers_2
    .filter(F.col("currency_max_element_dense_rank_3") != 4)
    .select(
        "id",
        "currency",
        "element",
        "element_dense_rank",
        "currency_avg_element",
        "element_dense_rank_2",
        "element_dense_rank_2_share",
        "currency_stddev_samp_element_dense_rank_2",
        "currency_sum_element",
        "element_dense_rank_2_rank",
        "currency_max_element_dense_rank_2_share",
        "currency_max_element_dense_rank_3",
        "currency_count_element",
        "currency_avg_element_rank",
        (F.col("element_dense_rank") / F.nullif("element_dense_rank_2_share", F.lit(0))).alias("element_dense_rank_per_element_dense_rank_2_share"),
    )
)

totals = (
    base
    .rollup(
        F.when(F.col("currency_sum_element") >= 370, "PREMIUM").otherwise("STANDARD").alias("currency_sum_element_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("element_dense_rank_2_share", 0.9).alias("element_dense_rank_2_share_p90"),
    )
)

result = (
    totals
    .select("currency_sum_element_tier", "usd_events", "element_dense_rank_2_share_p90")
    .orderBy(F.col("usd_events").desc_nulls_first(), "currency_sum_element_tier")
)
