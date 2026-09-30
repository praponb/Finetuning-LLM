from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(130, 370) & F.col("order_id").isin([20, 27]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total_2"),
    )
)
