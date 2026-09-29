from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.avg("rating").alias("avg_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.max("rating").alias("max_rating"),
    )
    .orderBy("rating_range", F.col("max_rating").desc())
)
