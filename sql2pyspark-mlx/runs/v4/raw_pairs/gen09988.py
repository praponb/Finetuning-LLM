from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("unit_price_bucket", F.col("cnt").desc())
)
