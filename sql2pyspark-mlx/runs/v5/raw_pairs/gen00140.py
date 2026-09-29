from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
    .filter(F.col("singapore_count") >= 3)
)
