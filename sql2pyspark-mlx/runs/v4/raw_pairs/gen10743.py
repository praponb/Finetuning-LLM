from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
