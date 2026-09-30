from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.age") > 64) | (F.col("o.channel") == "mobile"))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("c.credit_limit"),
    )
)
