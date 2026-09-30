from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12934, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .orderBy("tokyo_sqft")
)
