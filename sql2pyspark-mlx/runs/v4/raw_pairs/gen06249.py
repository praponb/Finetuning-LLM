from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.supplier_id").isNull())
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
        F.col("products.category"),
        F.col("products.brand"),
        F.floor(F.col("order_items.quantity")).alias("quantity_whole"),
    )
)
