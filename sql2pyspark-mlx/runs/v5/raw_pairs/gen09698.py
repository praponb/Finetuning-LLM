from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 8)
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") < 65)
    .select("rating_bucket", "country_list", "stddev_rating")
)
