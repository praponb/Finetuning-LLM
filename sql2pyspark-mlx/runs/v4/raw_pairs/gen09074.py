from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency")

flattened = (
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
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank_2"),
    )
)

with_windows_price_tiers = (
    with_windows
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w3)), 4))
    .withColumn("plan_min_element_dense_rank_2", F.min("element_dense_rank_2").over(w3))
    .withColumn("currency_min_element", F.min("element").over(w4))
)

result = (
    with_windows_price_tiers
    .groupBy("plan")
    .agg(
        F.round(F.avg("currency_min_element"), 2).alias("avg_currency_min_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("element_per_eur"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.round(F.sum("element_dense_rank_2") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("element_dense_rank_2_per_thb"),
    )
)
