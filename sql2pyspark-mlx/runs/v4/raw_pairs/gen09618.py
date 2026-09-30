from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
    .orderBy("channel")
)
