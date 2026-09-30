from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "quantity",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_band"),
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_adjusted"),
    )
)
