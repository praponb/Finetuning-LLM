from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("stddev_rating") > 77)
    .orderBy("stddev_rating")
)
