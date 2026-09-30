from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        (F.max(F.col("orders.shipping_fee")) - F.min(F.col("orders.shipping_fee"))).alias("shipping_fee_range"),
        F.max(F.col("orders.order_date")).alias("last_order_date"),
    )
    .filter(F.col("shipping_fee_range") > 42)
    .orderBy("signup_month")
)
