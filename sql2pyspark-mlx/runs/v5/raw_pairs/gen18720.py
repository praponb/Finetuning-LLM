from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.channel") == "partner") | (F.col("o.shipping_fee") >= 12))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
        F.col("c.is_active"),
        F.date_format(F.col("p.paid_at"), "yyyy-MM").alias("paid_at_label"),
    )
)
