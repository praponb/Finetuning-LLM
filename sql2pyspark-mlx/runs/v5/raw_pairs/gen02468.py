from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.status") == "LOST", F.col("s.cost")).otherwise(0)).alias("lost_cost"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("lost_cost").desc(), "status")
    .limit(1)
)
