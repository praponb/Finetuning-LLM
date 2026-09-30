from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")
w3 = Window.partitionBy("plan", "currency").orderBy("entry_price", "id").rowsBetween(-2, -1)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("avg_tier_price_bucket"),
        F.avg("entry_price").over(w3).alias("entry_price_avg_2_prior"),
    )
)
