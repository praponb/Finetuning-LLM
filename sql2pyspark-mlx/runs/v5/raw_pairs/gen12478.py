from pyspark.sql import functions as F

src = df_orders.filter(F.col("order_date") <= "2025-10-11")

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.count("*").alias("total_count"),
    )
)

result = (
    status_summary
    .select("status", "min_order_date", "total_count")
    .orderBy(F.col("min_order_date").desc(), "status")
    .limit(20)
)
