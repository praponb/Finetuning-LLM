from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("status") == "IN_TRANSIT")

summary = (
    recent_shipments
    .groupBy("carrier")
    .agg(F.min("delivered_date").alias("min_delivered_date"))
)

result = (
    summary
    .select("carrier", "min_delivered_date")
    .orderBy("carrier")
    .limit(10)
)
