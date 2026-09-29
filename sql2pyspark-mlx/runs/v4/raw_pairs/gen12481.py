from pyspark.sql import functions as F

result = (
    df_suppliers
    .rollup(
        F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL").alias("rating_tier"),
        "city",
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("rating_per_uk"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("rating"))).alias("tokyo_avg_rating"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating"))).alias("singapore_total_rating"),
    )
)
