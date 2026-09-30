from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.status").alias("order_status"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
)

result = dim_totals.select("order_status", "shipment_status", "rows_total", "max_cost")
