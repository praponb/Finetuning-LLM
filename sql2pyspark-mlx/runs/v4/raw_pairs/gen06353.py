from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("rating_stddev") < 0)
)
