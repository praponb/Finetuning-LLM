from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.city"))
    .agg(F.round(F.avg(F.col("orders.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") > 80)
    .orderBy("city")
)
