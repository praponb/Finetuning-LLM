from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("stores.city"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("orders.store_id"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.date_sub(F.col("stores.opened_date"), 64).alias("opened_date_minus_64d"),
    )
)
