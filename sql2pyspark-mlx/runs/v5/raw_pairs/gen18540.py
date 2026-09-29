from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") >= 3))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

result = (
    valid
    .select("order_id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id") == 22)
        .select("order_id", "product_id")
    )
)
