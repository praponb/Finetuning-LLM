from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 7) & (F.col("unit_price") >= 290))
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)
