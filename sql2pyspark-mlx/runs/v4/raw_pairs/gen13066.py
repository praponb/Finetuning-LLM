from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.avg("rating").alias("avg_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("stddev_rating").desc())
)
