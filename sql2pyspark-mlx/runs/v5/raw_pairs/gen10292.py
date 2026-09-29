from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan").orderBy(F.col("plan_min_element").asc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_min_element").desc_nulls_first())
w4 = Window.partitionBy("plan")
w5 = Window.orderBy(F.col("plan_min_element").desc_nulls_first())
w6 = Window.orderBy(F.col("plan_min_element_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("plan_count_plan_min_element").desc_nulls_first())
w8 = Window.orderBy(F.col("plan_count_plan_min_element_percent_rank").asc_nulls_first())
w9 = Window.orderBy(F.col("plan_min_element_rank_dense_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("plan_min_element_rank_dense_rank").asc_nulls_first())
w11 = Window.orderBy("plan_min_element_rank_dense_rank")
w12 = Window.orderBy(F.col("plan_min_element_cume_dist_minus_plan_min_element_dense_rank").desc_nulls_first())
w13 = Window.orderBy(F.col("plan_min_element_rank_dense_rank").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

metrics = exploded.select("id", "plan", F.min("element").over(w).alias("plan_min_element"))

with_windows = (
    metrics
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        F.cume_dist().over(w2).alias("plan_min_element_cume_dist"),
        F.dense_rank().over(w3).alias("plan_min_element_dense_rank"),
        F.count("plan_min_element").over(w4).alias("plan_count_plan_min_element"),
        F.rank().over(w5).alias("plan_min_element_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan_min_element_dense_rank",
        "plan_count_plan_min_element",
        "plan_min_element_rank",
        (F.col("plan_min_element_cume_dist") - F.col("plan_min_element_dense_rank")).alias("plan_min_element_cume_dist_minus_plan_min_element_dense_rank"),
    )
)

with_windows_price_tiers = (
    cleaned
    .select(
        "id",
        "plan_count_plan_min_element",
        "plan_min_element_cume_dist_minus_plan_min_element_dense_rank",
        F.dense_rank().over(w6).alias("plan_min_element_rank_dense_rank"),
        F.percent_rank().over(w7).alias("plan_count_plan_min_element_percent_rank"),
    )
)

enriched = (
    with_windows_price_tiers
    .select(
        "id",
        "plan_min_element_cume_dist_minus_plan_min_element_dense_rank",
        "plan_min_element_rank_dense_rank",
        F.percent_rank().over(w8).alias("plan_count_plan_min_element_percent_rank_percent_rank"),
        F.percent_rank().over(w9).alias("plan_min_element_rank_dense_rank_percent_rank"),
    )
)

with_windows_price_tiers_2 = (
    enriched
    .select(
        "id",
        "plan_min_element_rank_dense_rank",
        "plan_count_plan_min_element_percent_rank_percent_rank",
        "plan_min_element_rank_dense_rank_percent_rank",
        F.rank().over(w10).alias("plan_min_element_rank_dense_rank_rank"),
        F.rank().over(w11).alias("plan_min_element_rank_dense_rank_rank_2"),
        F.percent_rank().over(w12).alias("plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank"),
        F.cume_dist().over(w13).alias("plan_min_element_rank_dense_rank_cume_dist"),
        F.cume_dist().over(w13).alias("plan_min_element_rank_dense_rank_cume_dist_2"),
    )
)

summary = (
    with_windows_price_tiers_2
    .rollup(
        F.when(F.col("plan_min_element_rank_dense_rank_rank_2") >= 9, "PREMIUM").otherwise("STANDARD").alias("plan_min_element_rank_dense_rank_rank_2_tier"),
        F.when(F.col("plan_min_element_rank_dense_rank_percent_rank") >= 0.67, "HIGH").otherwise("LOW").alias("plan_min_element_rank_dense_rank_percent_rank_tier"),
        F.when(F.col("plan_count_plan_min_element_percent_rank_percent_rank") >= 0.71, "HIGH").otherwise("LOW").alias("plan_count_plan_min_element_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank"), 2).alias("avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank"),
        F.percentile_approx("plan_min_element_rank_dense_rank", 0.95).alias("plan_min_element_rank_dense_rank_p95"),
    )
    .filter(F.col("avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank") < 1000000)
)

result = (
    summary
    .select(
        "plan_min_element_rank_dense_rank_percent_rank_tier",
        "plan_count_plan_min_element_percent_rank_percent_rank_tier",
        "avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank",
    )
    .orderBy(
        F.col("plan_count_plan_min_element_percent_rank_percent_rank_tier").desc_nulls_first(),
        "avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank",
    )
)
