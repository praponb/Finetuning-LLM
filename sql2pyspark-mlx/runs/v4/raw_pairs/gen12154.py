from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("orders.order_date") > "2023-09-01")
    .groupBy(F.col("stores.id"))
    .agg(F.count(F.col("orders.id")).alias("order_count"))
    .orderBy(F.col("id").desc())
)
