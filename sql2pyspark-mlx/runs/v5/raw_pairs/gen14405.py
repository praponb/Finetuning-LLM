from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("currency").orderBy("entry_price", "id")
w3 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .filter(F.col("entry_price") <= 24)
    .select(
        "id",
        "currency",
        F.avg("entry_price").over(w).alias("entry_price_avg_3_prior"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(3).over(w2).alias("entry_price_bucket"),
        F.last("entry_price", ignorenulls=True).over(w3).alias("last_known_entry_price"),
        F.sum("entry_price").over(w3).alias("cumulative_entry_price"),
    )
)
