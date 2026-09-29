from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.age") == 66)
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("orders.customer_id"),
        F.col("customers.credit_limit"),
        F.col("customers.email"),
    )
)
