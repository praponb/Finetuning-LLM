from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("products.supplier_id"),
        F.col("products.brand"),
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(20)
)
