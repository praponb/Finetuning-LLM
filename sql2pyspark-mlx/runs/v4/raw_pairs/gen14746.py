from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.segment"))
    .agg(
        F.sum(F.when(F.col("orders.status") == "RETURNED", F.col("orders.discount")).otherwise(0)).alias("returned_discount"),
        F.sum(F.col("orders.amount")).alias("total_amount"),
    )
    .filter(F.col("total_amount") > 9500)
    .orderBy("total_amount", "segment")
    .limit(1)
)
