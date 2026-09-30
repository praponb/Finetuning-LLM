from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")
w2 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_lowest_price", F.last("lowest_price", ignorenulls=True).over(w2))
)
