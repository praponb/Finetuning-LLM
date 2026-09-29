from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "UK"]))
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") < 3.5)
    .select("city", "min_rating", "avg_rating")
)
