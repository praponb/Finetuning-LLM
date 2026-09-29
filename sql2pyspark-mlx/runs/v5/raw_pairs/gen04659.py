from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_statuss"))
    .filter(F.col("unique_statuss") > 3)
    .orderBy("unique_statuss", "is_active")
    .limit(3)
)
