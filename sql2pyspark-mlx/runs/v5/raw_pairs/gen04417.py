from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & (F.col("product_id").isNull() | (F.col("quantity") < 3)))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    summary
    .filter(F.col("min_quantity") >= 3.6)
    .orderBy("min_quantity")
)
