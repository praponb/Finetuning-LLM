from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 20)
    .groupBy("status")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 8)
    .orderBy("status", F.col("row_count").desc())
)
