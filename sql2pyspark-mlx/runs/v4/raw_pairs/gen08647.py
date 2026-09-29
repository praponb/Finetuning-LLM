from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"))
    .filter(F.col("in_transit_count") > 5)
    .orderBy(F.col("delivered_month").desc())
    .limit(5)
)
