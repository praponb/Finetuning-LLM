from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating"))).alias("chiang_mai_total_rating"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("rating_per_th"),
    )
    .orderBy("avg_rating")
)
