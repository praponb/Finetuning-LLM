from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") > 37)
    .groupBy(F.col("c.country"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy("country")
)
