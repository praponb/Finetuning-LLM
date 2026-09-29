from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .filter((F.length(F.col("products.product_name")) >= 10) & F.col("orders.customer_id").isin([21, 23, 33]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.least(F.col("orders.discount"), F.col("orders.amount")).alias("least_discount_amount"),
    )
)
