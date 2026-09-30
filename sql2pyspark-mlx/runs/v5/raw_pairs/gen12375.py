from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("stores.opened_date"),
        F.col("customers.credit_limit"),
        F.upper(F.trim(F.col("customers.email"))).alias("email_normalized"),
    )
    .orderBy("email_normalized", F.col("id").desc())
)
