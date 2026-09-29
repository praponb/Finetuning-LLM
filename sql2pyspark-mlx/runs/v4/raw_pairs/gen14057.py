from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.avg(F.when(F.col("country") == "US", F.col("rating"))).alias("us_avg_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
    )
)
