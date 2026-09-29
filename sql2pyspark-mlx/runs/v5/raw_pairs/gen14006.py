from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("status") == "DELIVERED").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .filter(F.col("o.status").isin(["RETURNED", "SHIPPED"]))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
    .orderBy("channel", "id")
    .limit(10)
)
