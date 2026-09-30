from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w_ordered = Window.partitionBy("plan", "currency").orderBy("avg_tier_price", "id")
w_ordered_2 = Window.partitionBy("currency", "plan").orderBy("avg_tier_price", "id").rowsBetween(-2, Window.currentRow)
w_ordered_3 = Window.partitionBy("plan").orderBy("lowest_price", "id")
w5 = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency") != "EUR")
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "plan",
        "currency",
        F.row_number().over(w_recent).alias("row_seq"),
        F.lead("lowest_price", 2, 0).over(w_ordered).alias("next_lowest_price"),
        F.avg("avg_tier_price").over(w_ordered_2).alias("avg_tier_price_avg_2"),
        F.row_number().over(w_ordered_3).alias("row_seq_2"),
        F.sum("avg_tier_price").over(w5).alias("cumulative_avg_tier_price"),
    )
)
