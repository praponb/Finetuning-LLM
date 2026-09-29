from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "currency")
w4 = Window.orderBy(F.col("element").asc_nulls_last())
w5 = Window.orderBy(F.col("element").desc_nulls_first())
w6 = Window.orderBy(F.col("element_rank").asc_nulls_last())
w_hist = Window.partitionBy("currency").orderBy(F.col("plan_max_element").desc_nulls_first())
w_ordered = Window.orderBy(F.col("element_dense_rank").asc_nulls_last())
w9 = Window.partitionBy("currency", "plan").orderBy(F.col("plan_max_element").desc())
w10 = Window.partitionBy("currency").orderBy(F.col("plan_max_element_dense_rank").desc_nulls_last())
w11 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank_rank").asc_nulls_first())
w12 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").asc_nulls_last())
w13 = Window.partitionBy("currency")
w14 = Window.orderBy("plan_stddev_samp_element")
w_grp = Window.partitionBy("currency").orderBy("plan_stddev_samp_element")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

scored = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.stddev_samp("element").over(w2).alias("plan_stddev_samp_element"),
        F.max("element").over(w3).alias("plan_max_element"),
        F.rank().over(w4).alias("element_rank"),
        F.percent_rank().over(w5).alias("element_percent_rank"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.percent_rank().over(w6).alias("element_rank_percent_rank"),
        F.dense_rank().over(w_hist).alias("plan_max_element_dense_rank"),
        F.rank().over(w_ordered).alias("element_dense_rank_rank"),
        F.rank().over(w9).alias("plan_max_element_rank"),
    )
)

scored_price_tiers = (
    metrics
    .withColumn("plan_max_element_dense_rank_cume_dist", F.cume_dist().over(w10))
    .withColumn("element_dense_rank_rank_dense_rank", F.dense_rank().over(w11))
)

metrics_price_tiers = (
    scored_price_tiers
    .select(
        "*",
        F.percent_rank().over(w12).alias("element_percent_rank_percent_rank"),
        F.round((F.col("element_dense_rank_rank") / F.sum("element_dense_rank_rank").over(w13)), 4).alias("element_dense_rank_rank_share"),
        F.dense_rank().over(w14).alias("plan_stddev_samp_element_dense_rank"),
        F.percent_rank().over(w_grp).alias("plan_stddev_samp_element_percent_rank"),
    )
)

filtered = (
    metrics_price_tiers
    .filter(F.col("element_dense_rank_rank") == 7)
    .select(
        "*",
        F.col("plan_stddev_samp_element").cast("string").alias("plan_stddev_samp_element_num"),
        F.round("plan_stddev_samp_element_percent_rank", 1).alias("plan_stddev_samp_element_percent_rank_rounded"),
    )
)

result = (
    filtered
    .rollup("plan")
    .agg(
        F.round(F.sum("element_dense_rank_rank") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("element_dense_rank_rank_per_starter"),
        F.round(F.avg("plan_stddev_samp_element"), 2).alias("avg_plan_stddev_samp_element"),
        F.count("*").alias("row_count"),
    )
)
