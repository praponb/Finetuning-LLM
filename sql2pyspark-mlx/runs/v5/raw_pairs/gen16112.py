from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("adjusted_val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("currency_max_val").desc())
w4 = Window.orderBy(F.col("adjusted_val_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

filtered = (
    lines
    .filter(F.col("val") <= 47)
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        F.round((F.col("val") * 100), 2).alias("adjusted_val"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.max("val").over(w).alias("currency_max_val"),
        F.rank().over(w2).alias("adjusted_val_rank"),
    )
)

filtered_price_tiers = (
    enriched
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-13")))
    .select(
        "currency",
        "val",
        "currency_max_val",
        "adjusted_val_rank",
        F.ceil("val").alias("val_whole"),
    )
)

cleaned = (
    filtered_price_tiers
    .filter(F.col("currency_max_val").isNotNull())
    .select(
        "currency",
        "currency_max_val",
        "adjusted_val_rank",
        "val_whole",
        F.when(F.col("val") >= 71, "High").when(F.col("val") >= 26, "Medium").otherwise("Low").alias("val_band"),
        (F.col("adjusted_val_rank") + F.col("currency_max_val")).alias("adjusted_val_rank_plus_currency_max_val"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.rank().over(w3).alias("currency_max_val_rank"),
        F.percent_rank().over(w4).alias("adjusted_val_rank_percent_rank"),
        F.max("val_whole").over(w).alias("currency_max_val_whole"),
    )
)

cleaned_price_tiers = (
    metrics
    .filter(F.col("currency") != "EUR")
    .select(
        "*",
        (F.col("adjusted_val_rank") - F.col("val_whole")).alias("adjusted_val_rank_minus_val_whole"),
    )
)

result = (
    cleaned_price_tiers
    .groupBy("currency")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.max(F.when(F.col("currency") == "USD", F.col("val_whole"))).alias("usd_max_val_whole"),
        F.avg("val_whole").alias("avg_val_whole"),
        F.max(F.when(F.col("currency") == "THB", F.col("adjusted_val_rank_minus_val_whole"))).alias("thb_max_adjusted_val_rank_minus_val_whole"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .orderBy("eur_rows")
)
