from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([25, 29]))
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

agg = (
    base
    .groupBy(
        F.when(F.col("quantity_rounded") >= 79, "high").when(F.col("quantity_rounded") >= 27, "mid").otherwise("low").alias("quantity_rounded_bucket"),
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    agg
    .filter(F.col("avg_unit_price") >= 20)
    .select(
        "quantity_rounded_bucket",
        "unit_price_bucket",
        "min_unit_price",
        "avg_unit_price",
    )
    .orderBy(
        F.col("min_unit_price").desc(),
        "unit_price_bucket",
        "quantity_rounded_bucket",
    )
    .limit(20)
)
