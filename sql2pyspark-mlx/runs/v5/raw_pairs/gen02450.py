from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_quantity") >= 4.3)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("t.avg_quantity"),
        F.col("t.unit_price_count"),
    )
)
