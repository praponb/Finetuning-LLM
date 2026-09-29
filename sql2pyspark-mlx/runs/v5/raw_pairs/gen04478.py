from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") <= 4) & (F.col("product_id") == 37))
    .select(
        "id",
        "quantity",
        "product_id",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)
