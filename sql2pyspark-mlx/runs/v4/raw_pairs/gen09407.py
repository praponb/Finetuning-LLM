from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.avg(F.col("s.cost")).alias("avg_cost"))
    .orderBy("avg_cost", "status")
    .limit(10)
)
