from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
    .filter(F.col("cost_count") >= 3)
    .orderBy(F.col("cost_count").desc())
)
