from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
    .filter(F.col("th_rating") > 36)
    .select("city", "country", "min_rating", "th_rating")
)
