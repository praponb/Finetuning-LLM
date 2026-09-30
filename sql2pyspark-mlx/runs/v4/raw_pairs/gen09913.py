from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
    )
    .filter(F.col("rating_p90") < 0)
    .orderBy("country")
)
