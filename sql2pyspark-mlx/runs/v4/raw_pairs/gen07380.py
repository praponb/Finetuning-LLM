from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.7, "HIGH").otherwise("LOW"))
    .cube("rating_tier", "country")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_countries") > 1)
)
