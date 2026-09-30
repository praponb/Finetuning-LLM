from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("product_id") == 19) & (F.col("unit_price") <= 180)) | (F.col("unit_price") >= 130))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") >= 170)
)
