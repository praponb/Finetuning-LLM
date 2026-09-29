from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("country").isin(["SG", "US", "JP"]))
    .rollup(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy(F.col("rating_range").desc())
)
