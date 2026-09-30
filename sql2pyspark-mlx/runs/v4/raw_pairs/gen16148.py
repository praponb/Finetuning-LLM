from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency").orderBy("tier_value")
w3 = Window.partitionBy("currency")
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w5 = Window.orderBy("tier_value")
w6 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

scored = (
    exploded
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w3))
)

filtered = (
    scored
    .filter(F.col("tier_value_dense_rank") > 4)
    .select(
        "*",
        (F.col("currency_max_tier_value") * F.col("tier_value")).alias("currency_max_tier_value_times_tier_value"),
    )
)

base = (
    filtered
    .filter(F.col("tier_value_dense_rank") > 3)
    .select(
        "*",
        F.round((F.col("currency_max_tier_value_times_tier_value") * 1.07), 2).alias("adjusted_currency_max_tier_value_times_tier_value"),
    )
)

with_windows = (
    base
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w4))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w5))
    .withColumn("tier_value_cume_dist_rank", F.rank().over(w6))
)

result = (
    with_windows
    .rollup(
        F.when(F.col("tier_value_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("tier_value_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.max("tier_value_dense_rank").alias("max_tier_value_dense_rank"),
        F.count("*").alias("cnt"),
        F.percentile_approx("currency_max_tier_value", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_quartiles"),
    )
)
