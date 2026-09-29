from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("channel") != "mobile").alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.city") != "Chiang Mai")
    .select(F.col("c.id"), F.col("c.segment"))
    .orderBy("segment")
)
