from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("rating"))).alias("chiang_mai_avg_rating"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)
