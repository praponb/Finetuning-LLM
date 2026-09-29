from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 7) & (F.col("quantity") >= 4))
    .select(
        "id",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)
