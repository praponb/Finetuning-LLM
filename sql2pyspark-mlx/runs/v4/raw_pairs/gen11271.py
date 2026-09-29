from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 4)
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .orderBy("unique_products", F.col("product_id").desc(), "quantity_bucket")
    .limit(20)
)
