from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_recent = Window.orderBy("element")
w3 = Window.orderBy(F.col("element").asc_nulls_last())
w_part = Window.orderBy(F.col("element_percent_rank").desc_nulls_last())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").desc_nulls_first())
w_ordered = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w8 = Window.partitionBy("currency").orderBy(F.col("currency_sum_element").desc_nulls_last())
w9 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank").desc())
w10 = Window.partitionBy("currency", "plan")
w_running = Window.partitionBy("plan").orderBy(F.col("element").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

enriched = (
    lines
    .select(
        "*",
        F.sum("element").over(w).alias("currency_sum_element"),
        F.percent_rank().over(w_recent).alias("element_percent_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("element_percent_rank_percent_rank", F.percent_rank().over(w_part))
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w5)), 4))
    .withColumn("element_dense_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("currency_avg_element", F.avg("element").over(w))
    .withColumn("currency_max_currency_sum_element", F.max("currency_sum_element").over(w))
)

filtered = (
    enriched_price_tiers
    .filter(F.col("currency_sum_element").between(339, 350))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_sum_element",
        "element_dense_rank",
        "element_percent_rank_percent_rank",
        "element_dense_rank_percent_rank",
        "currency_avg_element",
        "currency_max_currency_sum_element",
        (F.col("element_dense_rank_percent_rank") + F.col("currency_sum_element")).alias("element_dense_rank_percent_rank_plus_currency_sum_element"),
    )
)

enriched_price_tiers_2 = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("element_cume_dist"),
        F.avg("element").over(w).alias("currency_avg_element_2"),
        F.stddev_samp("currency_max_currency_sum_element").over(w5).alias("plan_stddev_samp_currency_max_currency_sum_element"),
    )
)

scored = (
    enriched_price_tiers_2
    .select(
        "*",
        F.dense_rank().over(w8).alias("currency_sum_element_dense_rank"),
        F.dense_rank().over(w9).alias("element_dense_rank_dense_rank"),
        F.stddev_samp("element").over(w10).alias("currency_stddev_samp_element"),
        F.rank().over(w_running).alias("element_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_sum_element",
        "element_dense_rank",
        "element_percent_rank_percent_rank",
        "currency_avg_element",
        "currency_max_currency_sum_element",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(starter|pro)$"), "EXTENDED").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("element_dense_rank").desc_nulls_last())
)
