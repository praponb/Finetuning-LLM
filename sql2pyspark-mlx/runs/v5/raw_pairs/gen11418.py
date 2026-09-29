from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
)
