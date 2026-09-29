from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 26, 39]) & F.col("unit_price").between(230, 290))
    .select(
        "id",
        "unit_price",
        "quantity",
        "order_id",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)
