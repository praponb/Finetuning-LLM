from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.2)
    .groupBy("city", "country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("jp_rating") > 67)
    .select("city", "country", "jp_rating", "rating_count")
    .orderBy(F.col("rating_count").desc(), "city", "country")
    .limit(10)
)
