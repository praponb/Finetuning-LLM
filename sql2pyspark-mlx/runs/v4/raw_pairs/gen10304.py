from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 6, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc())
)
