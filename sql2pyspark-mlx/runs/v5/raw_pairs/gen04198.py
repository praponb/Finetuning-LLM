from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "outer")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("order_items.order_id").isin([8, 18, 32]) & F.col("suppliers.city").isin(["Tokyo", "London"]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.product_id"),
        F.col("products.stock_qty"),
        F.col("products.category"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
    )
)
