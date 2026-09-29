from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("region") == "East").select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)
