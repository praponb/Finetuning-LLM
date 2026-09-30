from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("quantity") < 4)
    .select("*", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)

result = (
    valid
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id") == 3)
        .select("order_id", "product_id")
    )
)
