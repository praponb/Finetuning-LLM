from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("city").desc())
)
