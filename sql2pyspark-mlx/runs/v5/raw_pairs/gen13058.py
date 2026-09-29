from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("effective_from", "id")
w2 = Window.partitionBy("currency").orderBy("entry_price", "id")
w3 = Window.orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("effective_from") <= "2024-04-29")
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.row_number().over(w).alias("row_seq"),
        F.lead("entry_price", 1).over(w2).alias("next_entry_price"),
        F.first("entry_price", ignorenulls=True).over(w3).alias("first_known_entry_price"),
    )
)
