from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .filter(F.col("unit_price_range") > 56)
    .orderBy(F.col("unit_price_range").desc())
)
