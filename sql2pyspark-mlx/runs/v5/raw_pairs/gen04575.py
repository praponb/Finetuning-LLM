from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("delivered_date").alias("latest_delivered_date"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
    .orderBy("shipped_quarter")
)
