from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("effective_from_plus_17d", F.date_add("effective_from", 17))
    .withColumn("effective_from_period", F.date_trunc("WEEK", "effective_from"))
)

metrics = (
    filtered
    .filter(F.col("effective_from_plus_17d") <= "2024-07-25")
    .select(
        "id",
        "effective_from",
        "effective_from_plus_17d",
        F.avg("tier_value").over(w).alias("tier_value_avg_90d"),
        F.count("tier_value").over(w2).alias("tier_value_count_7d"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
        F.avg("tier_value").over(w4).alias("tier_value_avg_90d_2"),
    )
)

filtered_price_tiers = (
    metrics
    .filter(F.col("tier_value_avg_90d") >= 62)
    .select("*", F.last_day("effective_from").alias("effective_from_month_end"))
)

result = (
    filtered_price_tiers
    .select(
        "id",
        "effective_from_plus_17d",
        "tier_value_avg_90d",
        "tier_value_count_7d",
        "tier_value_avg_90d_2",
    )
    .orderBy(F.col("effective_from_plus_17d").asc_nulls_first(), "tier_value_count_7d")
)
