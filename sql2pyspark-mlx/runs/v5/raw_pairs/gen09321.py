from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
    .orderBy("tokyo_count")
)
