from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([3, 32, 39]) & (F.col("quantity") < 3) & (F.col("quantity") <= 8)))
    .select(
        "id",
        "unit_price",
        "order_id",
        F.when(F.col("unit_price") >= 430, "High").when(F.col("unit_price") >= 60, "Medium").otherwise("Low").alias("unit_price_tier"),
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
)
