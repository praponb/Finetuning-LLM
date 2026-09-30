from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "product_id",
        "quantity",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)
