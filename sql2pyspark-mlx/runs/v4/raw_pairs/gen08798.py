from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("cost").between(54, 80))
    .withColumn("status_upper", F.upper("status"))
)

carrier_summary = (
    filtered_shipments
    .groupBy("carrier")
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)

result = carrier_summary.select("carrier", "min_shipped_date")
