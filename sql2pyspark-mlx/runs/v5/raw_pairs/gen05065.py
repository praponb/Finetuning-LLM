from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("element").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.orderBy("currency_max_element")
w7 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w8 = Window.orderBy(F.col("element_rank_2").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

scored = (
    flattened
    .filter(F.col("element").isNotNull())
    .select(
        "plan",
        "currency",
        "element",
        F.rank().over(w).alias("element_rank"),
        F.rank().over(w2).alias("element_rank_2"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
        F.rank().over(w4).alias("element_rank_3"),
        F.max("element").over(w5).alias("currency_max_element"),
    )
)

metrics = (
    scored
    .select(
        "plan",
        "currency",
        "element_rank",
        "element_rank_2",
        F.percent_rank().over(w6).alias("currency_max_element_percent_rank"),
        F.dense_rank().over(w7).alias("element_dense_rank"),
    )
)

scored_price_tiers = (
    metrics
    .select(
        "plan",
        "element_rank_2",
        F.cume_dist().over(w8).alias("element_rank_2_cume_dist"),
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w5)), 4).alias("element_dense_rank_share"),
    )
)

filtered = (
    scored_price_tiers
    .filter(F.col("element_rank_2_cume_dist") <= 0.57)
    .withColumn("element_dense_rank_share_minus_element_rank_2_cume_dist", (F.col("element_dense_rank_share") - F.col("element_rank_2_cume_dist")))
    .withColumn("plan_group", F.when(F.col("plan").isin(["starter", "enterprise"]), "Group A").otherwise("Other"))
)

grouped = (
    filtered
    .groupBy("plan")
    .agg(
        F.percentile_approx("element_rank_2", 0.5).alias("element_rank_2_p50"),
        F.percentile_approx("element_rank_2", [0.25, 0.5, 0.75]).alias("element_rank_2_quartiles"),
        F.round(F.sum("element_rank_2_cume_dist") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("element_rank_2_cume_dist_per_pro"),
        F.round(F.sum("element_rank_2") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("element_rank_2_per_enterprise"),
    )
)

result = (
    grouped
    .select(
        "plan",
        "element_rank_2_p50",
        "element_rank_2_cume_dist_per_pro",
        "element_rank_2_per_enterprise",
    )
    .orderBy(F.col("element_rank_2_per_enterprise").desc_nulls_first(), "plan")
)
