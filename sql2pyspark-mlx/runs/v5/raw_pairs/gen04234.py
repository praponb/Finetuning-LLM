from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("customers.credit_limit") <= 6100)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("customers.signup_date"),
        F.col("stores.city"),
        F.col("customers.is_active").cast("int").alias("is_active_flag"),
    )
)
