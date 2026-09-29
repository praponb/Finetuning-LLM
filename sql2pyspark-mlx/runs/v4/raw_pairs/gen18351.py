from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("stores.opened_date"),
        F.col("orders.store_id"),
        F.col("orders.order_date"),
    )
    .orderBy("order_date")
)
