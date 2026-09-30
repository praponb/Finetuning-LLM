from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id").isin([10, 26, 27, 31]))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

summary = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("greatest_quantity_unit_price") >= 65, "high").when(F.col("greatest_quantity_unit_price") >= 24, "mid").otherwise("low").alias("greatest_quantity_unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    summary
    .orderBy(
        F.col("min_unit_price").desc(),
        "product_id",
        "greatest_quantity_unit_price_bucket",
    )
    .limit(10)
)
