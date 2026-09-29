from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel") == "mobile")
    .groupBy(F.col("o.channel"))
    .agg(
        F.avg(F.col("s.cost")).alias("avg_cost"),
        F.count(F.when(F.col("s.status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
)
