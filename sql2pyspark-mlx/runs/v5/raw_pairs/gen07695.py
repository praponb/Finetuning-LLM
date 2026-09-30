from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w_running = Window.partitionBy("currency", "plan").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w4 = Window.partitionBy("currency", "plan").orderBy(F.col("element_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

scored = (
    flattened
    .filter(F.col("element") >= 54)
    .select(
        "*",
        F.avg("element").over(w).alias("currency_avg_element"),
        F.cume_dist().over(w_running).alias("element_cume_dist"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)

enriched = scored.withColumn("element_cume_dist_dense_rank", F.dense_rank().over(w4))

base = (
    enriched
    .filter(F.col("element_cume_dist_dense_rank") != 5)
    .select(
        "*",
        (F.col("element_cume_dist") * F.col("element_cume_dist_2")).alias("element_cume_dist_times_element_cume_dist_2"),
        F.round("element_cume_dist_dense_rank", 2).alias("element_cume_dist_dense_rank_rounded"),
    )
)

scored_price_tiers = (
    base
    .select(
        "*",
        F.stddev_samp("currency_avg_element").over(w5).alias("currency_stddev_samp_currency_avg_element"),
    )
)

result = (
    scored_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "currency_avg_element",
        "element_cume_dist",
        "element_cume_dist_2",
        "element_cume_dist_dense_rank",
        "element_cume_dist_times_element_cume_dist_2",
        "currency_stddev_samp_currency_avg_element",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "GROUP_B").otherwise("EXTENDED").alias("plan_zone"),
    )
    .orderBy("plan_zone")
)
