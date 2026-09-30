from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Suppl%"))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("berlin_count") > 42)
    .orderBy("berlin_count", "city_values", "rating_bucket")
    .limit(3)
)
