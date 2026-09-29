from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element_percent_rank").desc())
w3 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("element").desc())
w5 = Window.orderBy(F.col("element_percent_rank_2").desc())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("element"))
)

with_windows = exploded.select("*", F.percent_rank().over(w).alias("element_percent_rank"))

with_windows_price_tiers = (
    with_windows
    .filter(F.col("element") <= 70)
    .withColumn("element_percent_rank_rank", F.rank().over(w2))
)

filtered = (
    with_windows_price_tiers
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-04-19")))
    .withColumn("effective_from_period", F.date_trunc("month", "effective_from"))
)

scored = (
    filtered
    .select(
        "id",
        "element",
        "element_percent_rank",
        "element_percent_rank_rank",
        F.count("element").over(w3).alias("element_count_7d"),
        F.percent_rank().over(w4).alias("element_percent_rank_2"),
    )
)

metrics = scored.select("*", F.cume_dist().over(w5).alias("element_percent_rank_2_cume_dist"))

result = (
    metrics
    .select(
        "id",
        "element",
        "element_percent_rank_rank",
        "element_count_7d",
        "element_percent_rank_2",
        "element_percent_rank_2_cume_dist",
    )
    .orderBy("element_count_7d")
)
