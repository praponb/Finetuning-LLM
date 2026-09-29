from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_sum_element").desc_nulls_last())
w6 = Window.orderBy(F.col("element").desc_nulls_last())
w7 = Window.orderBy("currency_sum_element_percent_rank")
w8 = Window.partitionBy("currency", "plan")
w9 = Window.partitionBy("plan", "currency")
w10 = Window.orderBy(F.col("plan_stddev_samp_element").desc_nulls_last())
w_grp = Window.orderBy(F.col("currency_sum_element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

with_windows = (
    exploded
    .withColumn("plan_stddev_samp_element", F.stddev_samp("element").over(w_recent))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("currency_sum_element", F.sum("element").over(w3))
    .withColumn("plan_max_element", F.max("element").over(w_recent))
)

with_windows_price_tiers = (
    with_windows
    .withColumn("element_cume_dist", F.cume_dist().over(w4))
    .withColumn("currency_sum_element_percent_rank", F.percent_rank().over(w5))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w6))
)

cleaned = (
    with_windows_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-05-13").cast("date"))
    .withColumn("element_cume_dist_minus_currency_sum_element_percent_rank", (F.col("element_cume_dist") - F.col("currency_sum_element_percent_rank")))
)

scored = (
    cleaned
    .withColumn("currency_sum_element_percent_rank_rank", F.rank().over(w7))
    .withColumn("currency_stddev_samp_element", F.stddev_samp("element").over(w8))
    .withColumn("plan_min_currency_sum_element_percent_rank", F.min("currency_sum_element_percent_rank").over(w9))
)

enriched = (
    scored
    .withColumn("currency_count_plan_max_element", F.count("plan_max_element").over(w3))
    .withColumn("plan_stddev_samp_element_dense_rank", F.dense_rank().over(w10))
    .withColumn("currency_sum_element_cume_dist", F.cume_dist().over(w_grp))
)

cleaned_price_tiers = (
    enriched
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-29")))
    .select(
        "plan",
        "effective_from",
        "currency",
        "plan_stddev_samp_element",
        "element_percent_rank",
        "currency_sum_element",
        "plan_max_element",
        "element_cume_dist",
        "currency_sum_element_percent_rank",
        "element_cume_dist_2",
        "element_cume_dist_minus_currency_sum_element_percent_rank",
        "currency_sum_element_percent_rank_rank",
        "currency_stddev_samp_element",
        "plan_min_currency_sum_element_percent_rank",
        "plan_stddev_samp_element_dense_rank",
        "currency_sum_element_cume_dist",
        F.round("element_percent_rank", 1).alias("element_percent_rank_rounded"),
        F.round((F.col("currency_sum_element_cume_dist") * 1.15), 2).alias("adjusted_currency_sum_element_cume_dist"),
    )
)

result = (
    cleaned_price_tiers
    .withColumn("year_start", F.date_trunc("year", "effective_from"))
    .cube("year_start")
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("currency_sum_element_percent_rank_rank"))).alias("pro_total_currency_sum_element_percent_rank_rank"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.percentile_approx("element_percent_rank_rounded", [0.25, 0.5, 0.75]).alias("element_percent_rank_rounded_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("distinct_currencies") >= 1)
    .orderBy(F.col("year_start").desc_nulls_last())
)
