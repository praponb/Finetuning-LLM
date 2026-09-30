from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"), "status")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
    .filter(F.col("lost_cost") >= 49)
    .orderBy("first_delivered_date", "delivered_quarter", "status")
    .limit(10)
)
