from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("effective_from_quarter").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("days_since_effective_from_month_end").asc_nulls_last())
w3 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

valid = (
    lines
    .filter(F.col("currency") == "USD")
    .select("*", F.last_day("effective_from").alias("effective_from_month_end"))
)

cleaned = (
    valid
    .filter(F.year("effective_from") >= 2025)
    .select(
        "effective_from",
        "currency",
        "val",
        F.when(F.col("currency").isin(["THB", "USD"]), "Group A").otherwise("Other").alias("currency_group"),
        F.datediff(F.current_date(), "effective_from_month_end").alias("days_since_effective_from_month_end"),
    )
)

valid_price_tiers = (
    cleaned
    .filter(F.col("val") <= 79)
    .select("*", F.quarter("effective_from").alias("effective_from_quarter"))
)

enriched = (
    valid_price_tiers
    .filter(F.col("days_since_effective_from_month_end") != 50)
    .select(
        "val",
        "days_since_effective_from_month_end",
        F.cume_dist().over(w).alias("effective_from_quarter_cume_dist"),
        F.cume_dist().over(w2).alias("days_since_effective_from_month_end_cume_dist"),
        F.count("effective_from_quarter").over(w3).alias("currency_count_effective_from_quarter"),
    )
)

result = (
    enriched
    .select(
        "val",
        "days_since_effective_from_month_end",
        "days_since_effective_from_month_end_cume_dist",
        "currency_count_effective_from_quarter",
    )
    .orderBy(F.col("currency_count_effective_from_quarter").desc_nulls_last())
)
