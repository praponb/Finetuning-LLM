from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum("rating").alias("total_rating"),
    )
)
