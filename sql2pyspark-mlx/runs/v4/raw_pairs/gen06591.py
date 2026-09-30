from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price").between(240, 270) & F.col("unit_price").between(130, 400)) | F.col("order_id").isin([36, 38, 39]))
    .select(
        "id",
        "product_id",
        "order_id",
        F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 120, "Medium").otherwise("Low").alias("unit_price_tier"),
        F.floor("quantity").alias("quantity_whole"),
    )
)
