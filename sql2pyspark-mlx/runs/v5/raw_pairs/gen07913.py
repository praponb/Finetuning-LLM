from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("orders.discount") <= 0.29)
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("orders.status") == "SHIPPED", F.col("orders.amount")).otherwise(0)).alias("shipped_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("orders.channel")))).alias("channel_values"),
    )
    .orderBy(F.col("signup_quarter").desc(), F.col("shipped_amount").desc())
)
