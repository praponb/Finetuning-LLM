from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.quantity").between(4, 7))
    .groupBy(
        F.when(F.col("o.amount") >= 1230, "high").when(F.col("o.amount") >= 450, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 250)
    .orderBy(F.col("avg_unit_price").desc())
)
