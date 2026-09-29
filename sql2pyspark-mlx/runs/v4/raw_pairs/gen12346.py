from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 340)
    .select(
        "product_id",
        "quantity",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc(), F.col("quantity").desc())
)
