from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .filter(F.col("plan").isin(["pro", "starter", "basic"]))
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.lag("entry_price").over(w).alias("prev_entry_price"),
        F.lead("entry_price").over(w).alias("next_entry_price"),
        F.row_number().over(w).alias("seq_no"),
    )
)
