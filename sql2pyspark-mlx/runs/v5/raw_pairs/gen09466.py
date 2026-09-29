from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.avg(F.when(F.col("city") == "London", F.col("rating"))).alias("london_avg_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
    )
    .filter(F.col("london_avg_rating") > 0)
    .orderBy(F.col("london_avg_rating").desc_nulls_last())
)
