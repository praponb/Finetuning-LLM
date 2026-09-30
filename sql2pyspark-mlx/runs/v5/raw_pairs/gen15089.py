from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.cost")).alias("max_cost"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)
