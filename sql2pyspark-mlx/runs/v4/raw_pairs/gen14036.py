from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter((F.year(F.col("stores.opened_date")) >= 2023) & F.col("shipments.carrier").isin(["Kerry", "FedEx", "Flash"]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("orders.order_date"),
        F.col("orders.amount"),
        F.col("stores.sqft"),
        F.col("stores.region"),
    )
    .orderBy("sqft")
)
