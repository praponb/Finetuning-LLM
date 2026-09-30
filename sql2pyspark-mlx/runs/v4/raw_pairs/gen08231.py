from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("quantity") != 5) | F.col("order_id").isin([15, 22, 23, 25]))
)

quantity_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("num_order_items"))
)

result = quantity_bucket_summary.orderBy("quantity_bucket")
