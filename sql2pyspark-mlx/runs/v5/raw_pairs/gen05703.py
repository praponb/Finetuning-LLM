from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 16, 22, 32]))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("max_unit_price") > 110)
    .drop("max_unit_price")
    .orderBy("quantity_bucket")
)
