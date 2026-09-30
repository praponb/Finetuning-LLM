from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"))
    .orderBy("ups_count")
)
