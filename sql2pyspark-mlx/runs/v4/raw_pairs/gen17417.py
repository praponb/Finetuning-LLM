from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("unit_price") >= 300)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

result = (
    valid
    .select("product_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") > 5)
        .select("product_id", "id")
    )
)
