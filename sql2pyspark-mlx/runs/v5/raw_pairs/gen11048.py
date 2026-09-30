from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["order_status", "shipment_status"], ["order_status"]],
        F.col("p.status").alias("order_status"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.cost")).alias("max_cost"),
    )
    .orderBy("max_cost")
)
