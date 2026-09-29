from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
