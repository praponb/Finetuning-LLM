from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(230, 410))
    .groupBy(
        F.when(F.col("price") >= 1350, "high").when(F.col("price") >= 580, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.countDistinct("cost").alias("unique_categorys"))
    .filter(F.col("unique_categorys") < 2)
    .orderBy("price_bucket")
)
