from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .filter(F.col("customers.city") == "Singapore")
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("customers.signup_date"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
    )
)
