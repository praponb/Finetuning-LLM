from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("stores.store_name").like("Si%"))
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(F.max(F.col("orders.discount")).alias("max_discount"))
)
