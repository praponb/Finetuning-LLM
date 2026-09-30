from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.min_cost") >= 52)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("ot.min_cost"),
        F.col("ot.returned_cost"),
    )
    .orderBy("channel")
)
