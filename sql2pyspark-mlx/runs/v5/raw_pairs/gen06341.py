from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 18) & (F.col("unit_price") > 200))
    .select(
        "id",
        "order_id",
        "unit_price",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("quantity_adjusted").desc())
)
