from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"), F.col("o.status"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .orderBy(F.col("order_year").desc())
)
