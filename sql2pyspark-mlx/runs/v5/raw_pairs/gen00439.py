from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w2 = Window.partitionBy("currency").orderBy("entry_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "avg_tier_price",
        "entry_price",
        F.row_number().over(w).alias("row_seq"),
        F.lag("lowest_price", 1).over(w2).alias("prev_lowest_price"),
    )
)
