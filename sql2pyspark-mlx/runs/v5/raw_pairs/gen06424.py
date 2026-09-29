from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & ~F.col("country").isin(["UK", "TH", "US"]))
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
)
