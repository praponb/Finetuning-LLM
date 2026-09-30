from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .withColumn("clean_city", F.trim("city"))
)

suppliers_stats = (
    valid_suppliers
    .groupBy("city")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("stddev_rating") >= 25)
    .select("city", "stddev_rating", "max_rating")
    .orderBy("stddev_rating")
)
