from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.avg("rating").alias("avg_rating"))
    .filter(F.col("avg_rating") >= 3.5)
    .orderBy("avg_rating")
)
