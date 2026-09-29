from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(~F.col("payments.status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        F.col("payments.order_id"),
        F.col("payments.method"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.col("customers.age"),
    )
    .orderBy(F.col("store_id").desc(), "method")
)
