from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")
w4 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("avg_tier_price", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        "lowest_price",
        F.sum("lowest_price").over(w).alias("running_lowest_price"),
        F.rank().over(w2).alias("lowest_price_rank"),
        F.ntile(5).over(w3).alias("avg_tier_price_bucket"),
        F.last("entry_price", ignorenulls=True).over(w4).alias("last_known_entry_price"),
        F.lead("avg_tier_price", 1).over(w5).alias("next_avg_tier_price"),
    )
)
