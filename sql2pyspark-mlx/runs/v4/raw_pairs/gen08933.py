from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 19)
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("quantity_range") < 70)
    .select("unit_price_bucket", "order_id", "quantity_range", "total_unit_price")
    .orderBy(F.col("total_unit_price").desc(), "order_id", "unit_price_bucket")
    .limit(5)
)
