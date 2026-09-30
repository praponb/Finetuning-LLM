from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.discount") >= 0.28)
    .groupBy(F.col("orders.customer_id"), F.col("orders.channel"))
    .agg(
        F.avg(F.col("shipments.cost")).alias("avg_cost"),
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("avg_cost").desc())
)
