from pyspark.sql import functions as F

base = df_shipments.filter(F.col("delivered_date") <= "2025-01-25")

order_id_summary = (
    base
    .groupBy("order_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = order_id_summary.select("order_id", "avg_cost", "stddev_cost")
