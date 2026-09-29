from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("orders.order_date") > "2024-11-01")
    .select(
        F.col("stores.city"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
        F.col("orders.channel"),
    )
    .orderBy(F.col("channel").desc())
)
