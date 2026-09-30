from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy("tier_value")
w3 = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("currency_group", F.when(F.col("currency").isin(["EUR", "USD"]), "Core").otherwise("Other"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank_2"),
        F.sum("tier_value").over(w3).alias("currency_sum_tier_value"),
        F.dense_rank().over(w).alias("tier_value_dense_rank_3"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("tier_value") >= 82, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_dense_rank", 0.95).alias("tier_value_dense_rank_p95"),
        F.percentile_approx("currency_sum_tier_value", 0.95).alias("currency_sum_tier_value_p95"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.round(F.avg("tier_value_dense_rank"), 2).alias("avg_tier_value_dense_rank"),
        F.round(F.avg("tier_value_dense_rank_3"), 2).alias("avg_tier_value_dense_rank_3"),
    )
    .orderBy(F.col("avg_tier_value_dense_rank_3").desc())
)
