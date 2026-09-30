from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .groupBy(
        F.col("c.country"),
        F.when(F.col("c.credit_limit") >= 15500, "high").when(F.col("c.credit_limit") >= 3100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.avg(F.col("p.amount")).alias("avg_amount"))
    .orderBy("avg_amount")
)
