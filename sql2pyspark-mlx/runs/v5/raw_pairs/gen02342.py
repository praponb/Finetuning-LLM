from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter(F.col("products.price") > 960)
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.order_id"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
    .orderBy(F.col("unit_price").desc())
)
