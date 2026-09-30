from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.amount")).alias("amount_count"),
        F.count(F.when(F.col("p.status") == "FAILED", 1)).alias("failed_count"),
    )
)
