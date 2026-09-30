from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sum("rating").alias("total_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
    )
    .filter(F.col("singapore_count") > 1)
)
