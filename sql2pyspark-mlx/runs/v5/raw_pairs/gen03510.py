from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("order_id") == 18).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.amount").between(530, 780))
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.status"), F.col("o.order_date"))
    .orderBy(F.col("id").desc())
)
