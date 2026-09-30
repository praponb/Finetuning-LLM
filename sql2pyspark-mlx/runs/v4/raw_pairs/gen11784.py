from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("c.email"),
        F.col("c.city"),
    )
)
