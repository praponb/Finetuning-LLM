from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") > 60)
    .select("shipped_month", "latest_delivered_date", "avg_cost")
    .orderBy(F.col("latest_delivered_date").desc())
)
