from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc())
w_ordered = Window.partitionBy("plan")
w_running = Window.partitionBy("plan").orderBy(F.col("element_rank").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("currency_avg_element").desc_nulls_last())
w6 = Window.orderBy(F.col("currency_min_element").desc())
w7 = Window.orderBy(F.col("currency_sum_currency_min_element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.min("element").over(w).alias("currency_min_element"),
        F.rank().over(w2).alias("element_rank"),
        F.sum("element").over(w).alias("currency_sum_element"),
        F.avg("element").over(w).alias("currency_avg_element"),
    )
)

metrics = (
    enriched
    .withColumn("plan_sum_currency_avg_element", F.sum("currency_avg_element").over(w_ordered))
)

with_windows = (
    metrics
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "currency_sum_element",
        "currency_avg_element",
        "plan_sum_currency_avg_element",
        F.percent_rank().over(w_running).alias("element_rank_percent_rank"),
        F.count("element_rank").over(w_ordered).alias("plan_count_element_rank"),
        F.dense_rank().over(w5).alias("currency_avg_element_dense_rank"),
        F.dense_rank().over(w6).alias("currency_min_element_dense_rank"),
        F.sum("currency_min_element").over(w).alias("currency_sum_currency_min_element"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "plan",
        "effective_from",
        "currency_sum_element",
        "currency_avg_element",
        "plan_sum_currency_avg_element",
        "element_rank_percent_rank",
        "currency_min_element_dense_rank",
        "currency_sum_currency_min_element",
        F.round((F.col("plan_count_element_rank") / F.sum("plan_count_element_rank").over(w_ordered)), 4).alias("plan_count_element_rank_share"),
        F.count("currency_avg_element").over(w_ordered).alias("plan_count_currency_avg_element"),
        F.dense_rank().over(w7).alias("currency_sum_currency_min_element_dense_rank"),
    )
)

result = (
    with_windows_price_tiers
    .select(
        "plan",
        "effective_from",
        "currency_sum_element",
        "plan_sum_currency_avg_element",
        "currency_sum_currency_min_element",
        "plan_count_element_rank_share",
        "plan_count_currency_avg_element",
        "currency_sum_currency_min_element_dense_rank",
        F.when(F.col("plan").rlike("^(pro|basic)$"), "GROUP_A").when(F.col("plan").rlike("^(starter|enterprise)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
)
