from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 22]) & (F.col("unit_price") >= 290) & F.col("product_id").isNull())
    .select(
        "order_id",
        "quantity",
        "product_id",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("order_id").desc(), F.col("line_total").desc())
)
