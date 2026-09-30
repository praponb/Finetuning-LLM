from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-10-23")
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy(F.col("total_cost").desc())
)
