from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("orders.amount") <= 660)
    .select(
        F.col("payments.status"),
        F.col("orders.amount"),
        F.col("customers.country"),
        F.col("customers.is_active"),
        F.last_day(F.col("orders.order_date")).alias("order_date_month_end"),
    )
)
