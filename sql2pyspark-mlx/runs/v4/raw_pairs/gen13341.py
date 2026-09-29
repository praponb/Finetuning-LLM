from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.min("rating").alias("min_rating"),
        F.count("*").alias("cnt"),
    )
)
