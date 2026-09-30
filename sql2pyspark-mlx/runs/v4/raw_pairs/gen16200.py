from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("customers.segment"))
    .agg(F.countDistinct(F.col("orders.discount")).alias("unique_channels"))
    .filter(F.col("unique_channels") > 4)
)
