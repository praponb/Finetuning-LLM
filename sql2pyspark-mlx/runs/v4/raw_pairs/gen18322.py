from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.delivered_count") >= 30)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("ot.delivered_count"),
        F.col("ot.carrier_list"),
    )
)
