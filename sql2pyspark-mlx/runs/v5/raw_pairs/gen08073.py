from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc(), "id")
w4 = Window.partitionBy("currency", "plan").orderBy(F.col("effective_from").desc(), "id")
w5 = Window.partitionBy("plan")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("avg_tier_price") < 20)
    .select(
        "*",
        F.first("avg_tier_price", ignorenulls=True).over(w).alias("first_known_avg_tier_price"),
        F.avg("avg_tier_price").over(w_ordered).alias("avg_tier_price_avg_6"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("avg_tier_price").over(w5).alias("plan_avg_avg_tier_price"),
    )
)
