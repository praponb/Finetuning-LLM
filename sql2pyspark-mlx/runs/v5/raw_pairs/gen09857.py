from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["mobile", "web", "store"]))
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("c.age"))
)
