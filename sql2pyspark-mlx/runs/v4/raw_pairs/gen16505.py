from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("order_items.quantity"),
        F.col("products.cost"),
        F.col("products.brand"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
    )
)
