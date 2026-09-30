from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("carrier").isNotNull() & F.col("cost").between(28, 67))

summary = (
    filtered_shipments
    .groupBy("status")
    .agg(F.max("delivered_date").alias("last_delivered_date"))
)

result = (
    summary
    .select("status", "last_delivered_date")
    .orderBy("status", "last_delivered_date")
    .limit(10)
)
