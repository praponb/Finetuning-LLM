from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w3 = Window.orderBy(F.col("element").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_element").desc_nulls_last())
w6 = Window.partitionBy("plan", "currency")
w7 = Window.orderBy(F.col("plan_stddev_samp_element_percent_rank").desc())
w8 = Window.partitionBy("currency").orderBy(F.col("element_rank").desc_nulls_first())
w9 = Window.partitionBy("currency", "plan").orderBy(F.col("plan_stddev_samp_element").desc_nulls_last())
w10 = Window.partitionBy("currency")
w11 = Window.orderBy("plan_stddev_samp_element_cume_dist")
w12 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").desc())
w_running = Window.orderBy(F.col("element_rank").desc())
w_ordered = Window.partitionBy("plan").orderBy(F.col("element_rank").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

with_windows = (
    flattened
    .select(
        "*",
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element"),
        F.rank().over(w2).alias("element_rank"),
        F.max("element").over(w).alias("plan_max_element"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
        F.rank().over(w4).alias("element_rank_2"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("plan_stddev_samp_element_percent_rank"),
        F.cume_dist().over(w5).alias("plan_stddev_samp_element_cume_dist"),
        F.stddev_samp("plan_stddev_samp_element").over(w6).alias("plan_stddev_samp_plan_stddev_samp_element"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("plan_stddev_samp_element_percent_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("element_rank_rank", F.rank().over(w8))
    .withColumn("plan_stddev_samp_element_dense_rank", F.dense_rank().over(w9))
)

enriched_price_tiers_2 = (
    enriched_price_tiers
    .select(
        "*",
        F.avg("plan_stddev_samp_element_cume_dist").over(w10).alias("currency_avg_plan_stddev_samp_element_cume_dist"),
        F.cume_dist().over(w11).alias("plan_stddev_samp_element_cume_dist_cume_dist"),
        F.dense_rank().over(w12).alias("element_dense_rank_dense_rank"),
    )
)

result = (
    enriched_price_tiers_2
    .select(
        "*",
        F.cume_dist().over(w_running).alias("element_rank_cume_dist"),
        F.cume_dist().over(w_ordered).alias("element_rank_cume_dist_2"),
    )
)
