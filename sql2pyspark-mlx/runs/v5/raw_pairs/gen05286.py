from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("element_rank").desc())
w4 = Window.orderBy(F.col("element_rank_share").desc_nulls_first())
w5 = Window.orderBy(F.col("element_rank_share_dense_rank").asc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

metrics = (
    lines
    .filter(F.col("plan").isin(["basic", "enterprise", "pro"]))
    .select("id", "plan", F.rank().over(w).alias("element_rank"))
)

metrics_price_tiers = (
    metrics
    .withColumn("plan_stddev_samp_element_rank", F.stddev_samp("element_rank").over(w2))
    .withColumn("element_rank_share", F.round((F.col("element_rank") / F.sum("element_rank").over(w2)), 4))
    .withColumn("element_rank_rank", F.rank().over(w3))
)

base = (
    metrics_price_tiers
    .filter(F.col("element_rank_share") <= 0.79)
    .select(
        "id",
        "plan",
        "element_rank",
        "element_rank_share",
        "element_rank_rank",
        F.trim("plan").alias("clean_plan"),
    )
)

metrics_price_tiers_2 = (
    base
    .select(
        "id",
        "plan",
        "element_rank",
        F.percent_rank().over(w4).alias("element_rank_share_percent_rank"),
        F.round((F.col("element_rank_rank") / F.sum("element_rank_rank").over(w2)), 4).alias("element_rank_rank_share"),
        F.dense_rank().over(w4).alias("element_rank_share_dense_rank"),
    )
)

metrics_price_tiers_3 = (
    metrics_price_tiers_2
    .withColumn("element_rank_share", F.round((F.col("element_rank") / F.sum("element_rank").over(w2)), 4))
    .withColumn("element_rank_share_dense_rank_share", F.round((F.col("element_rank_share_dense_rank") / F.sum("element_rank_share_dense_rank").over(w2)), 4))
    .withColumn("element_rank_share_dense_rank_rank", F.rank().over(w5))
)

result = (
    metrics_price_tiers_3
    .groupBy("plan")
    .agg(
        F.max("element_rank_rank_share").alias("max_element_rank_rank_share"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
    )
    .orderBy(F.col("max_element_rank_rank_share").desc_nulls_first())
)
