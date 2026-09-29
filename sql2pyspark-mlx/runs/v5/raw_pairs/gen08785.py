from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 190) & (F.col("product_id").isNotNull() | (F.col("unit_price") > 210)))
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
