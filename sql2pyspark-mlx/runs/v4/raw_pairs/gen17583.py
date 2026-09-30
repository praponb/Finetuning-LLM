from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy(F.col("entry_price").desc())
w2 = Window.orderBy(F.col("lowest_price").desc_nulls_first())

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.percent_rank().over(w_part).alias("entry_price_percent_rank"),
        F.dense_rank().over(w2).alias("lowest_price_dense_rank"),
    )
)
