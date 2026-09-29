from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("stores.sqft").between(6235, 7837))
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("region").desc())
)
