from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("unit_price") < 220)
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    summary
    .select("quantity_bucket", "total_quantity")
    .orderBy(F.col("total_quantity").desc())
)
