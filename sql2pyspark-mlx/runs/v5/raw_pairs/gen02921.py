from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.col("stores.store_name").like("Sia%") & (F.col("stores.sqft") == 8377))
    .select(
        F.col("shipments.id"),
        F.col("shipments.carrier"),
        F.col("orders.amount"),
        F.col("stores.region"),
    )
)
