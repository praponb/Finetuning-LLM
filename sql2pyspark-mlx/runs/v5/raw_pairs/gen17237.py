from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 1400)
    .orderBy("status")
    .limit(5)
)
