from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
    .orderBy("delivered_count")
)
