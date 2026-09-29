from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.order_date") <= "2025-04-02")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.amount"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
