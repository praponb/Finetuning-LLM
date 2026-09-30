from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.channel").isin(["store", "partner", "web"]))
    .groupBy(F.col("stores.id"), F.col("stores.store_name"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.min(F.col("orders.order_date")).alias("min_order_date"),
        (F.max(F.col("orders.discount")) - F.min(F.col("orders.discount"))).alias("discount_range"),
    )
    .orderBy("min_order_date", "id", "store_name")
    .limit(10)
)
