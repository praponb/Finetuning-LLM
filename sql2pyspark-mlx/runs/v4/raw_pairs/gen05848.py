from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 25) | (F.col("order_id") == 5))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .orderBy("product_id")
)
