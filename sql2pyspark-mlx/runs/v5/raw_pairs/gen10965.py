from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") >= 14) & F.col("country").isin(["UK", "SG"]) & F.col("country").isin(["TH", "JP"]))
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count("rating").alias("rating_count"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("rating_count") < 2)
)
