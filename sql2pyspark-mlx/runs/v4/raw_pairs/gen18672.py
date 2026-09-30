from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 370)
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
