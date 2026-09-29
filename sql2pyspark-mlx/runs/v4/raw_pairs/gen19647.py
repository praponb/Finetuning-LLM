from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.customer_id").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.order_date"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.initcap(F.trim(F.col("stores.region"))).alias("region_title"),
    )
)
