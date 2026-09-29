from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.min("rating").alias("min_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
