from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([29, 31, 36]))
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .orderBy("carrier", "latest_shipped_date")
)
