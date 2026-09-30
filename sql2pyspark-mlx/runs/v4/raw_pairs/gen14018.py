from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "order_id",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)
