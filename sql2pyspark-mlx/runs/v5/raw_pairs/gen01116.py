from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("order_date") < "2025-07-01").select("id")

result = (
    df_shipments.alias("shipments")
    .join(excluded_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("shipments.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("shipments.cost") <= 54)
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("shipments.carrier"),
        F.col("shipments.delivered_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
