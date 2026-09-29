from pyspark.sql import functions as F

result = (
    df_suppliers
    .cube(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)
