from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([17, 35]))
    .select("shipped_date", "carrier", "status", F.trim("status").alias("clean_status"))
    .orderBy("clean_status")
)
