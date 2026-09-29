from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") >= 3.3)
    .select("country", "max_rating")
)
