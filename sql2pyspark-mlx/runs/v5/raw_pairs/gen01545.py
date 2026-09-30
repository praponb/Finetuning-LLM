from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(~F.col("stores.city").isin(["New York", "London", "Tokyo"]))
    .groupBy(F.col("stores.city"))
    .agg(F.max(F.col("orders.order_date")).alias("latest_order_date"))
)
