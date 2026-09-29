from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
    )
    .filter(F.col("num_cities") > 1)
)
