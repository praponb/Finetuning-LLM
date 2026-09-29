from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .orderBy(F.col("num_countries").desc())
)
