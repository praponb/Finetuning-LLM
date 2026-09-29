from pyspark.sql import functions as F

base = df_order_items.filter(F.col("product_id").isNull())

summary = (
    base
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    summary
    .select("product_id", "min_quantity")
    .orderBy(F.col("min_quantity").desc(), "product_id")
    .limit(10)
)
