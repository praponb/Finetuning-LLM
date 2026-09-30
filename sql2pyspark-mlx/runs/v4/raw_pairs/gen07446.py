from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "Chiang Mai") | F.col("supplier_name").isNull())
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.min("rating").alias("min_rating"))
)
