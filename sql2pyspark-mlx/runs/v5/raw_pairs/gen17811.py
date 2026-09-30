from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("order_items.order_id").isin([21, 22, 23, 35]))
    .groupBy(F.col("products.supplier_id"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("supplier_id").desc())
)
