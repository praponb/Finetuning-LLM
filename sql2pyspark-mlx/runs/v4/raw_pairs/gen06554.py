from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("row_count").desc())
)
