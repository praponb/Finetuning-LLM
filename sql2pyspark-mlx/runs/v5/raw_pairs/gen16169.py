from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Glo%"))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.min("rating").alias("min_rating"),
    )
)
