from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("orders.status") != "PENDING") & (F.col("customers.country") == "DE"))
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.amount"),
        F.col("customers.is_active"),
        F.col("customers.credit_limit"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
    )
)
