from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 170)
    .select(
        "unit_price",
        F.when(F.col("unit_price") >= 400, "High").when(F.col("unit_price") >= 80, "Medium").otherwise("Low").alias("unit_price_level"),
    )
    .orderBy(F.col("unit_price").desc())
)
