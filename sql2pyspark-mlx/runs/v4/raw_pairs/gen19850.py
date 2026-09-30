from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("customer_id").isin([11, 17, 20])).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.col("c.city") != "Tokyo")
    .select(F.col("c.id"), F.col("c.segment"))
)
