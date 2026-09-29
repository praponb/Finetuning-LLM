from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.name").like("%Kim%"))
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.sort_array(F.collect_set(F.col("orders.channel"))).alias("channel_list"),
        F.max(F.col("orders.discount")).alias("max_discount"),
    )
    .orderBy(F.col("max_discount").desc())
)
