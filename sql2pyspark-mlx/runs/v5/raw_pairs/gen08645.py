from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(~F.col("currency").isin(["THB", "USD"]))
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)
