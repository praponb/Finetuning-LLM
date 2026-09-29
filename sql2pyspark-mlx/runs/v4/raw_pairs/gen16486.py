from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.status").isin(["SHIPPED", "COMPLETED"]), "core").otherwise("other").alias("status_group"),
        F.col("o.status"),
    )
    .agg(F.count("*").alias("total_count"))
)
