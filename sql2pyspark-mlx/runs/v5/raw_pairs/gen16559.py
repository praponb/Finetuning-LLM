from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.order_item_count") >= 6)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("t.order_item_count"),
    )
    .orderBy(F.col("order_item_count").desc(), F.col("stock_qty").desc())
)
