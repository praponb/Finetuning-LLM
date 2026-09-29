from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("country") != "UK").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.store_id") == 14)
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.channel"), F.col("o.discount"))
    .orderBy(F.col("discount").desc(), "id")
    .limit(50)
)
