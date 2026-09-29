from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("rating_per_bangkok"),
    )
)
