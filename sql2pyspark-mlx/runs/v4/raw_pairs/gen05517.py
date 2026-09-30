from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.region").isin(["East", "North"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
)
