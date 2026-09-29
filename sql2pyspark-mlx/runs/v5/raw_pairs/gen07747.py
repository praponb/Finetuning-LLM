from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter((F.col("products.cost") > 190) & (F.length(F.col("products.product_name")) >= 6))
    .select(
        F.col("products.category"),
        F.col("products.brand"),
        F.col("products.supplier_id"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
