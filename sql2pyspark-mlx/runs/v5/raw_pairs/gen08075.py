from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.countDistinct(F.col("o.amount")).alias("unique_customers"))
    .orderBy("unique_customers", "region")
)
