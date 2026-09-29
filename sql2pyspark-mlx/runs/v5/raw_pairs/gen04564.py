from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.order_date") >= "2024-12-01")
    .groupBy(F.col("c.segment"))
    .agg(F.avg(F.col("o.discount")).alias("avg_discount"))
)
