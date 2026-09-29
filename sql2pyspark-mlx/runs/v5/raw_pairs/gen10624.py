from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("customers.country") != "UK") & (F.col("payments.status") != "REFUNDED"))
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("orders.shipping_fee"),
        F.col("customers.credit_limit"),
        F.col("customers.signup_date"),
    )
)
