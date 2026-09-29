from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") < F.col("s.order_date")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
)
