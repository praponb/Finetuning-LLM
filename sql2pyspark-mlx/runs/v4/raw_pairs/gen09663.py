from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("unit_price").alias("min_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .orderBy("quantity_bucket")
)
