from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG", "DE"]) & ((F.col("rating") < 1.8) | F.col("rating").isNotNull()))
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("supplier_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") < 16)
    .drop("total_rating")
)
