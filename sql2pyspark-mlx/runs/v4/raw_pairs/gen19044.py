from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") > 9)
    .orderBy("min_rating", "total_rating")
)
