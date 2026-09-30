from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("unit_price") >= 140) & (F.col("quantity").isNull() | (F.col("product_id") == 34))))
    .select(
        "quantity",
        "order_id",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier_2"),
    )
)
