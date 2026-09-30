from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(~(F.lower(F.col("c.name")).like("%ob %") | (F.col("c.country") == "TH")))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.col("c.segment"),
        F.col("c.is_active").cast("int").alias("is_active_flag"),
    )
)
