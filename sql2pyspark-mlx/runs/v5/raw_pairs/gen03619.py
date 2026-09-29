from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.7) & (F.col("rating") <= 2.8) & ~F.col("city").isin(["London", "Tokyo", "Bangkok"]))
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)
