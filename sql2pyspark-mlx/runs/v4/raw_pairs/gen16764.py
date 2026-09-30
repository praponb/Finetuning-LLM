from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 3)
    .select("country", "rating_bucket", "us_count", "row_count")
)
