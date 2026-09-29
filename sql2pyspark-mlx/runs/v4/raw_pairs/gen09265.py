from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w_hist = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w_recent = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w4 = Window.orderBy(F.col("element").asc_nulls_first())
w_grp = Window.partitionBy("plan").orderBy(F.col("element_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

enriched = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.percent_rank().over(w_hist).alias("element_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("element") > 43)
    .select(
        "plan",
        "element",
        "element_cume_dist",
        (F.col("element_percent_rank") + F.col("element")).alias("element_percent_rank_plus_element"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("element_percent_rank"),
        F.dense_rank().over(w_recent).alias("element_dense_rank"),
        F.percent_rank().over(w4).alias("element_percent_rank_2"),
        F.dense_rank().over(w_grp).alias("element_cume_dist_dense_rank"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.max("element_cume_dist").over(w6).alias("plan_max_element_cume_dist"),
        F.max("element_cume_dist_dense_rank").over(w6).alias("plan_max_element_cume_dist_dense_rank"),
    )
)

filtered_price_tiers = (
    scored_price_tiers
    .filter(F.col("element_percent_rank") <= 0.63)
    .select(
        "plan",
        "element",
        "element_cume_dist",
        "element_percent_rank_plus_element",
        "element_percent_rank",
        "element_dense_rank",
        "element_percent_rank_2",
        "plan_max_element_cume_dist",
        "plan_max_element_cume_dist_dense_rank",
        F.coalesce("element_cume_dist", F.lit(0)).alias("element_cume_dist_filled"),
    )
)

cleaned = (
    filtered_price_tiers
    .filter(F.col("plan_max_element_cume_dist") == 0)
    .withColumn("element_dense_rank_filled", F.coalesce("element_dense_rank", F.lit(0)))
    .withColumn("element_plus_element_cume_dist_filled", (F.col("element") + F.col("element_cume_dist_filled")))
)

result = (
    cleaned
    .select(
        "plan",
        "element_cume_dist",
        "element_percent_rank_plus_element",
        "element_percent_rank",
        "element_dense_rank",
        "element_percent_rank_2",
        "plan_max_element_cume_dist",
        "element_cume_dist_filled",
        "element_dense_rank_filled",
    )
    .orderBy(F.col("element_percent_rank").asc_nulls_first())
)
