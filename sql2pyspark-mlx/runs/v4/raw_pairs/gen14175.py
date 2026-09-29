from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.order_id"),
        F.col("products.brand"),
        F.col("suppliers.city"),
    )
    .orderBy(F.col("order_id").desc())
)
