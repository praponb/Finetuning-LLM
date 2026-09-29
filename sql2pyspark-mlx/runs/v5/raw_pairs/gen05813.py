from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 4) | F.col("unit_price").between(170, 180))
    .select(
        "id",
        "product_id",
        "order_id",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
    .orderBy(F.col("id").desc())
)
