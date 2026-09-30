from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(~F.col("status").isin(["IN_TRANSIT", "RETURNED"])).alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_anti")
    .filter(F.col("orders.amount").isNotNull())
    .select(F.col("orders.id"), F.col("orders.shipping_fee"), F.col("orders.status"))
)
