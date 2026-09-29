from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.amount"),
        F.col("stores.opened_date"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
    )
    .orderBy("signup_date", "segment")
)
