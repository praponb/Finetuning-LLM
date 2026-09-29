from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w5 = Window.orderBy(F.col("tier_value_percent_rank").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-04-30")))
    .withColumn("currency_lower", F.lower("currency"))
)

metrics = (
    cleaned
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
)

result = (
    metrics
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("tier_value_percent_rank_rank", F.rank().over(w5))
)
