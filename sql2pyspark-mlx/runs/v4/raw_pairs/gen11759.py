from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("stores.id"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.sum(F.col("orders.amount")).alias("total_amount"),
    )
    .orderBy("total_amount")
)
