from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy("country")
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") >= 3.0)
    .select("country", "max_rating")
    .orderBy("country")
)
