from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("t.latest_delivered_date"),
        F.col("t.unique_carriers"),
    )
)
