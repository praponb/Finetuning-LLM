from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("o.status") != "PENDING") & F.col("c.name").like("%FRA%"))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("order_id").desc())
)
