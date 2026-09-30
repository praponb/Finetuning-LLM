from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.quantity_range") > 67)
    .select(
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("t.quantity_range"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("brand").desc())
)
