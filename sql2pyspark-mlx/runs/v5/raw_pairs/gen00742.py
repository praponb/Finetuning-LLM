from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNull() & F.col("product_id").isNotNull())
    .select(
        "order_id",
        "unit_price",
        "product_id",
        F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)
