from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") >= 5)
)
