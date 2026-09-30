from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.max("entry_price").over(w_hist).alias("last4_max_entry_price"),
        F.min("entry_price").over(w_hist).alias("last4_min_entry_price"),
        F.avg("entry_price").over(w_hist).alias("last4_avg_entry_price"),
    )
)
