from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") < 4)
    .orderBy(F.col("city").desc())
)
