from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1110, "high").when(F.col("price") >= 440, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") < 360)
    .orderBy("avg_cost")
)
