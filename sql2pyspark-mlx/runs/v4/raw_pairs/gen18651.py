from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("unit_price").alias("unit_price_count"))
    .filter(F.col("unit_price_count") > 5)
    .orderBy(F.col("unit_price_count").desc(), "quantity_bucket")
)
