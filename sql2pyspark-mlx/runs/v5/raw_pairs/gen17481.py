from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("entry_price", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.first("entry_price", ignorenulls=True).over(w).alias("first_known_entry_price"),
        F.ntile(4).over(w2).alias("entry_price_bucket"),
        F.ntile(3).over(w2).alias("entry_price_bucket_2"),
    )
)
