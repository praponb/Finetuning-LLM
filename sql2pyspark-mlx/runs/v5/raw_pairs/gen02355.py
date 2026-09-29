from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.customer_id").isNull())
    .groupBy(F.date_format(F.col("orders.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set(F.col("shipments.status"))).alias("status_list"),
        F.avg(F.col("shipments.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("order_month").desc())
)
