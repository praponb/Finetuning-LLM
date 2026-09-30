from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("p.amount") < 740)
    .groupBy(F.col("c.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc())
)
