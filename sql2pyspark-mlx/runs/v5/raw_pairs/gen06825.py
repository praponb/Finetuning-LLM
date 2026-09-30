from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("delivered_date") < "2023-10-27")

carrier_summary = (
    recent_shipments
    .groupBy("carrier", "status")
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
)

result = (
    carrier_summary
    .filter(F.col("delivered_count") > 8)
    .select("carrier", "delivered_count")
    .orderBy(F.col("delivered_count").desc(), "carrier")
)
