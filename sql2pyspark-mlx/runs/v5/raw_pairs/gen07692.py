from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("element").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w5 = Window.partitionBy("currency")

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
        "id",
        "effective_from",
        "currency",
        "element",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.rank().over(w2).alias("element_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
        F.rank().over(w4).alias("element_rank_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("currency") == "THB")
    .select(
        "*",
        F.date_add("effective_from", 82).alias("effective_from_plus_82d"),
        F.greatest("element_dense_rank", "element").alias("greatest_element_dense_rank_element"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "element_rank",
        "element_dense_rank",
        "effective_from_plus_82d",
        "greatest_element_dense_rank_element",
        F.sum("element_cume_dist").over(w5).alias("currency_sum_element_cume_dist"),
    )
)

totals = (
    metrics
    .groupBy(
        F.date_trunc("WEEK", "effective_from_plus_82d").alias("week_start"),
        F.when(F.col("element_rank") >= 7, "HIGH").otherwise("LOW").alias("element_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    totals
    .select("week_start", "element_rank_tier", "currency_list", "distinct_ids")
    .orderBy(F.col("distinct_ids").desc_nulls_last(), "week_start")
)
