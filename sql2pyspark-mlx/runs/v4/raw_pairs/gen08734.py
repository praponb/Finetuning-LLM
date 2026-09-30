from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_per_new_york"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count("*").alias("event_count"),
        F.min("rating").alias("min_rating"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("min_rating") > 0)
)
