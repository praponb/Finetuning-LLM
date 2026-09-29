from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Berlin")
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .orderBy(F.col("rating_bucket").desc(), F.col("unique_countrys").desc())
)
