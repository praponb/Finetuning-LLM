from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.first("lowest_price", ignorenulls=True).over(w_hist).alias("first_known_lowest_price"),
        F.dense_rank().over(w_recent).alias("lowest_price_dense_rank"),
        F.sum("lowest_price").over(w3).alias("running_lowest_price"),
    )
)
