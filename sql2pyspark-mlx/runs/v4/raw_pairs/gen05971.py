from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value_dense_rank_num").desc())
w4 = Window.orderBy(F.col("tier_value_dense_rank_num_filled").desc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

metrics = (
    flattened
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
)

filtered = (
    metrics
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        F.when(F.col("currency").isin(["USD", "EUR"]), "Core").otherwise("Other").alias("currency_group"),
        F.col("tier_value_dense_rank").cast("double").alias("tier_value_dense_rank_num"),
    )
)

cleaned = (
    filtered
    .filter(F.col("tier_value_dense_rank_num") < 65)
    .withColumn("tier_value_dense_rank_num_filled", F.coalesce("tier_value_dense_rank_num", F.lit(0)))
)

result = (
    cleaned
    .select(
        "*",
        F.rank().over(w3).alias("tier_value_dense_rank_num_rank"),
        F.cume_dist().over(w4).alias("tier_value_dense_rank_num_filled_cume_dist"),
        F.min("tier_value_dense_rank_num").over(w5).alias("currency_min_tier_value_dense_rank_num"),
        F.dense_rank().over(w6).alias("tier_value_cume_dist_dense_rank"),
    )
)
