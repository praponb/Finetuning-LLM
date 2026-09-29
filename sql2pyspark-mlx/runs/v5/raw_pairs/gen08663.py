from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([4, 8, 12]) & (F.col("carrier") == "Kerry"))
    .groupBy("status")
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy("last_delivered_date")
)
