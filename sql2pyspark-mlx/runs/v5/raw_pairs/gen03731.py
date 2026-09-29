from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") > 51)
    .select("city", "stddev_rating")
    .orderBy("stddev_rating")
)
