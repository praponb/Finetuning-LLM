from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(F.col("o.discount"), F.col("ot.lost_cost"))
)
