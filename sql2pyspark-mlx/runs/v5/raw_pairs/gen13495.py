from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.partitionBy("currency", "plan")
w3 = Window.partitionBy("plan", "currency")
w4 = Window.orderBy(F.col("is_eur").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

metrics = (
    exploded
    .select(
        "plan",
        "currency",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.max("tier_value").over(w_part).alias("currency_max_tier_value"),
    )
)

cleaned = (
    metrics
    .filter(F.col("currency") != "USD")
    .select(
        "*",
        F.round((F.col("currency_max_tier_value") * 0.9), 2).alias("currency_max_tier_value_scaled"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("tier_value_percent_rank").between(0.51, 0.63))
    .select(
        "*",
        F.coalesce("currency_max_tier_value_scaled", F.lit(0)).alias("currency_max_tier_value_scaled_filled"),
    )
)

cleaned_price_tiers_2 = (
    cleaned_price_tiers
    .filter(F.col("currency") == "USD")
    .select(
        "plan",
        "currency",
        "tier_value_percent_rank",
        "currency_max_tier_value",
        "currency_max_tier_value_scaled",
        "currency_max_tier_value_scaled_filled",
        F.when(F.col("currency") == "EUR", 1).otherwise(0).alias("is_eur"),
    )
)

scored = (
    cleaned_price_tiers_2
    .select(
        "currency",
        "currency_max_tier_value_scaled_filled",
        "is_eur",
        F.max("is_eur").over(w3).alias("plan_max_is_eur"),
        F.percent_rank().over(w4).alias("is_eur_percent_rank"),
    )
)

result = scored.select("is_eur", "plan_max_is_eur", "is_eur_percent_rank")
