from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 51)
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.avg(F.col("p.amount")).alias("avg_amount"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
    .orderBy("min_amount")
)
