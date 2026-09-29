from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("currency_max_element").asc_nulls_first())
w4 = Window.orderBy(F.col("element").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("plan_min_element").asc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank").asc_nulls_first())
w_ordered = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.min("element").over(w).alias("plan_min_element"),
        F.max("element").over(w2).alias("currency_max_element"),
    )
)

metrics = (
    with_windows
    .withColumn("currency_max_element_rank", F.rank().over(w3))
    .withColumn("element_rank", F.rank().over(w4))
    .withColumn("element_dense_rank", F.dense_rank().over(w5))
    .withColumn("plan_min_element_cume_dist", F.cume_dist().over(w6))
)

base = (
    metrics
    .filter(F.col("currency_max_element_rank") <= 7)
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

metrics_price_tiers = (
    base
    .withColumn("element_dense_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("currency_max_element_rank_share", F.round((F.col("currency_max_element_rank") / F.sum("currency_max_element_rank").over(w2)), 4))
    .withColumn("element_rank_share", F.round((F.col("element_rank") / F.sum("element_rank").over(w2)), 4))
)

metrics_price_tiers_2 = (
    metrics_price_tiers
    .withColumn("element_dense_rank_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("currency_count_element", F.count("element").over(w2))
)

by_type = (
    metrics_price_tiers_2
    .groupBy("currency")
    .pivot("plan", ["basic", "enterprise", "pro"])
    .agg(
        F.sum("element_dense_rank_percent_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)

result = (
    by_type
    .select(
        "currency",
        "basic_value_sum",
        "basic_n",
        "enterprise_value_sum",
        "enterprise_n",
        "pro_n",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "CORE").when(F.col("currency").rlike("^(EUR)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("currency_zone"),
    )
)
