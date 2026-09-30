from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 550, "high").when(F.col("cost") >= 250, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .orderBy("stddev_price")
)
