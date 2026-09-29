from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount") < 1220)
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(F.avg(F.col("o.discount")).alias("avg_discount"))
)
