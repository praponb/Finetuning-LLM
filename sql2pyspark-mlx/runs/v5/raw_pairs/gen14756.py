from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .select(
        "quantity",
        "order_id",
        F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 110, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)
