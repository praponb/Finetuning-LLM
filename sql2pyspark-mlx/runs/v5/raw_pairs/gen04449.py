from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 4) & (F.col("product_id") == 40))
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
    .orderBy(F.col("line_total").desc())
)
