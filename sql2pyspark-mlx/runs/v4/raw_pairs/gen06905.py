from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy("cost_count", "status")
    .limit(5)
)
