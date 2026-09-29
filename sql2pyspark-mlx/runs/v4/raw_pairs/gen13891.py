from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.discount") >= 0.13)
    .groupBy(
        F.when(F.col("o.amount") >= 1680, "high").when(F.col("o.amount") >= 340, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 3.3)
    .orderBy("amount_bucket")
)
