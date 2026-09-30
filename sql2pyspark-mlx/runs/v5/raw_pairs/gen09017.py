from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("o.store_id"), F.col("o.status"))
    .agg(
        F.sum(F.col("p.amount")).alias("total_amount"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy(F.col("total_amount").desc(), "store_id", "status")
    .limit(5)
)
