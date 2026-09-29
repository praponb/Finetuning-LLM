from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy("london_count", F.col("total_rating").desc())
)
