from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") >= 4)
    .drop("rating_count")
    .orderBy("rating_bucket")
)
