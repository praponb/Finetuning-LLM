from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .select(
        F.col("customers.city"),
        F.col("orders.status"),
        F.col("orders.store_id"),
        F.col("orders.amount"),
    )
)
