from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.4)
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("new_york_count") < 61)
    .select("rating_bucket", "new_york_count", "city_list")
    .orderBy(F.col("new_york_count").desc())
)
