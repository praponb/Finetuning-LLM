from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.1)
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy("total_rating", "country")
    .limit(50)
)
