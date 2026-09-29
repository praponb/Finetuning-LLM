from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .select(
        "unit_price",
        "product_id",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
