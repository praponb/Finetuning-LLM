from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 380)
    .select(
        "order_id",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
    .orderBy(F.col("order_id").desc())
)
