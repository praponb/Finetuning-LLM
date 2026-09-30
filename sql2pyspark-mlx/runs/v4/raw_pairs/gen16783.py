from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "UPS", "FedEx"]))
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("t.max_cost"),
        F.col("t.delivered_count"),
    )
    .orderBy("max_cost", "id")
)
