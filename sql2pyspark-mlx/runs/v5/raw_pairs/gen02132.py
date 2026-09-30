from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "avg_tier_price",
        F.avg("avg_tier_price").over(w).alias("avg_tier_price_avg_2"),
        F.first("avg_tier_price", ignorenulls=True).over(w2).alias("first_known_avg_tier_price"),
        F.last("avg_tier_price", ignorenulls=True).over(w_grp).alias("last_known_avg_tier_price"),
        F.dense_rank().over(w4).alias("avg_tier_price_dense_rank"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
