from pyspark.sql import functions as F

base = df_order_items.filter(F.col("quantity") < 3)

quantity_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)

result = (
    quantity_bucket_summary
    .filter(F.col("unique_orders") >= 5)
    .orderBy("quantity_bucket", F.col("unique_orders").desc())
)
