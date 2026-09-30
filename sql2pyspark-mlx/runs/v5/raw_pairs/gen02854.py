from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("os.total_cost"),
        F.col("os.dhl_cost"),
    )
    .orderBy(F.col("dhl_cost").desc())
)
