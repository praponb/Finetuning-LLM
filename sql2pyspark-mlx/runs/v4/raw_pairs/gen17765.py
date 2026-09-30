from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([21, 37]))
    .select(
        "id",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)
