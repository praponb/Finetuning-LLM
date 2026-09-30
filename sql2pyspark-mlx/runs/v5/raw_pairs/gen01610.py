from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)
