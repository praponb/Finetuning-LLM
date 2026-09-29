from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .orderBy("channel")
    .limit(20)
)
