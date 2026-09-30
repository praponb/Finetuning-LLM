from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("lowest_price").desc(), "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .filter(F.col("lowest_price") <= 21)
    .select(
        "id",
        "entry_price",
        F.max("avg_tier_price").over(w).alias("last3_max_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("last3_sum_avg_tier_price"),
        F.count("avg_tier_price").over(w).alias("last3_count_avg_tier_price"),
        F.ntile(3).over(w2).alias("lowest_price_bucket"),
    )
)
