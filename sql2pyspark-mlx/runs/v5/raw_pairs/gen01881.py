from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") > 3)
    .orderBy("rating_bucket", "city")
    .limit(3)
)
