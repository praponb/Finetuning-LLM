from pyspark.sql import functions as F

base = df_order_items.filter(F.col("order_id").isin([15, 28, 29]))

summary = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
)

result = (
    summary
    .filter(F.col("total_count") < 7)
    .select("product_id", "quantity_bucket", "total_count")
    .orderBy("product_id")
)
