from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)
