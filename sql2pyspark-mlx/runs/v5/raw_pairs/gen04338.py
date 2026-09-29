from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("order_id") == 28) & (F.col("order_id") == 23) & F.col("order_id").isin([12, 13, 16])))
    .select(
        "id",
        "quantity",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc())
)
