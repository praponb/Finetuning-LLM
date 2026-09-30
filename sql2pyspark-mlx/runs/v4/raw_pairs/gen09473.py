from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") < 51)
    .select("product_id", "quantity_bucket", "avg_unit_price")
    .orderBy(F.col("avg_unit_price").desc(), F.col("product_id").desc())
)
