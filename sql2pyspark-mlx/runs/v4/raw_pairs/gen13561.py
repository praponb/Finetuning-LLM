from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.discount") >= 0.29)
    .groupBy(
        F.when(F.col("orders.status").isin(["CANCELLED", "PENDING"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg(F.col("order_items.unit_price")), 2).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc())
)
