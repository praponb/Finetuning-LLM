from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount").between(0.2, 0.29))
    .groupBy(
        F.when(F.col("o.status").isin(["COMPLETED", "CANCELLED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.min(F.col("s.delivered_date")).alias("min_delivered_date"))
    .orderBy(F.col("min_delivered_date").desc())
)
