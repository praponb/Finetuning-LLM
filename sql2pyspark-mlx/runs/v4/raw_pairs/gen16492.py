from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([16, 33]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
    .orderBy(F.col("greatest_unit_price_quantity").desc())
)
