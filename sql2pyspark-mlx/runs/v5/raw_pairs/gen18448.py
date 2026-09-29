from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("quantity").between(5, 7))

order_id_summary = (
    recent_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    order_id_summary
    .select("order_id", "product_id", "quantity_count", "min_quantity")
    .orderBy(F.col("order_id").desc())
)
