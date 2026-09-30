from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.customer_id").isin([9, 11, 39]))
    .groupBy(
        F.col("orders.status"),
        F.year(F.col("orders.order_date")).alias("order_year"),
    )
    .agg(F.count(F.col("payments.amount")).alias("amount_count"))
)
