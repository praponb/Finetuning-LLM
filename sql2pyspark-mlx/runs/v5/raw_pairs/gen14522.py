from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([9, 14, 18, 33]) & F.col("order_id").isin([17, 23]))
)

unit_price_bucket_summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    unit_price_bucket_summary
    .filter(F.col("quantity_range") < 28)
    .select("unit_price_bucket", "revenue", "quantity_range")
)
