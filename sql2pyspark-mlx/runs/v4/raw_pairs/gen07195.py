from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_products"))
    .orderBy(F.col("product_id").desc(), "quantity_bucket")
    .limit(5)
)
