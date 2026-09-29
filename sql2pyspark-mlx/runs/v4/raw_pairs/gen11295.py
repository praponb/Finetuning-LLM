from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 7) | (F.col("product_id") == 15))
    .select(
        "unit_price",
        "product_id",
        "quantity",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
    .orderBy(F.col("least_quantity_unit_price").desc())
)
