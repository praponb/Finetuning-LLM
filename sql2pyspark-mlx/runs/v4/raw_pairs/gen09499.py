from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 6) & (F.col("order_id") == 20))
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total_2"),
    )
)
