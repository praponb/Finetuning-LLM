from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("entry_price")

parsed = (
    df_price_tiers
    .filter(~F.col("plan").isin(["pro", "enterprise", "starter"]))
    .select(
        "id",
        "plan",
        "effective_from",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "effective_from",
        "lowest_price",
        F.last("lowest_price", ignorenulls=True).over(w).alias("last_known_lowest_price"),
        F.rank().over(w2).alias("entry_price_rank"),
    )
)
