from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.order_id"),
        F.col("products.launch_date"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
    .orderBy(F.col("rating").desc())
)
