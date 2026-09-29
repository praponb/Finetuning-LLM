from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("order_items.order_id").isin([3, 30]))
    .groupBy(F.month(F.col("products.launch_date")).alias("launch_month"))
    .agg(F.count("*").alias("order_item_count"))
    .orderBy(F.col("order_item_count").desc(), F.col("launch_month").desc())
)
