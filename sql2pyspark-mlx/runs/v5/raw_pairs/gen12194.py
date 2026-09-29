from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"), "order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .orderBy(F.col("delivered_month").desc(), "order_id")
)
