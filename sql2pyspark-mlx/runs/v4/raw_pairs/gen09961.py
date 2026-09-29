from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(320, 330))
    .select(
        "id",
        "unit_price",
        "order_id",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)
