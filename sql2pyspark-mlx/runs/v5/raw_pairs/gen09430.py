from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .orderBy("unique_categorys", F.col("sports_count").desc())
)
