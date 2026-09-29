from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("s.status") == "LOST", F.col("s.cost")).otherwise(0)).alias("lost_cost"),
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
)
