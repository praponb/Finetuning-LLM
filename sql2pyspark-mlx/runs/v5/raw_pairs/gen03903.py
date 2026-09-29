from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("tier_value")
w3 = Window.orderBy(F.col("tier_value").desc())
w_part = Window.orderBy("currency_count_tier_value")
w5 = Window.orderBy("tier_value_rank")
w_recent = Window.orderBy(F.col("tier_value").desc_nulls_first())
w7 = Window.orderBy(F.col("tier_value_rank_percent_rank").desc_nulls_last())
w8 = Window.orderBy(F.col("tier_value_rank").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

metrics = (
    lines
    .filter(F.col("effective_from") < "2024-06-01")
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_rank", F.rank().over(w3))
)

filtered = (
    metrics
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "tier_value",
        "currency_count_tier_value",
        "tier_value_dense_rank",
        "tier_value_rank",
        F.add_months("effective_from", 12).alias("effective_from_plus_months"),
        F.when(F.col("tier_value_rank") >= 9, "High").when(F.col("tier_value_rank") >= 3, "Medium").otherwise("Low").alias("tier_value_rank_level"),
    )
)

with_windows = (
    filtered
    .withColumn("currency_count_tier_value_cume_dist", F.cume_dist().over(w_part))
    .withColumn("tier_value_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("tier_value_rank_2", F.rank().over(w_recent))
)

enriched = (
    with_windows
    .withColumn("tier_value_rank_percent_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("tier_value_rank_rank", F.rank().over(w8))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "effective_from_plus_months").alias("quarter_start"),
        F.when(F.col("tier_value_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("tier_value_dense_rank_tier"),
    )
    .agg(
        F.sum("currency_count_tier_value_cume_dist").alias("total_currency_count_tier_value_cume_dist"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
)
