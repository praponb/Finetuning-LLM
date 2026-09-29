from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isin([9, 38]) & (F.col("order_id").isin([1, 14, 18, 37]) | F.col("order_id").isin([7, 11, 29, 37])))
)

summary = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = summary.orderBy("unit_price_bucket")
