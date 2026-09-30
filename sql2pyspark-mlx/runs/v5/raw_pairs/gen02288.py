from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("entry_price").desc(), "id")
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("entry_price").desc_nulls_first())
w4 = Window.partitionBy("currency", "plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("currency").orderBy("entry_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "currency",
        "entry_price",
        F.sum("entry_price").over(w).alias("cumulative_entry_price"),
        F.ntile(10).over(w2).alias("entry_price_bucket"),
        F.dense_rank().over(w3).alias("entry_price_dense_rank"),
        F.sum("entry_price").over(w4).alias("cumulative_entry_price_2"),
        F.lag("entry_price", 1).over(w5).alias("prev_entry_price"),
    )
)
