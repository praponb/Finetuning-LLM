from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy("city", F.col("stddev_rating").desc())
)
