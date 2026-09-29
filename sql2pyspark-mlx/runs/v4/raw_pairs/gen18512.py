from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("customers.city").isin(["London", "Tokyo"]) & F.col("customers.age").between(46, 65))
    .select(
        F.col("orders.discount"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
        F.col("customers.city"),
        F.col("customers.country"),
    )
    .orderBy(F.col("country").desc())
)
