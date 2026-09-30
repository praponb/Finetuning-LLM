from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(3, 5))
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    valid
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") == 3)
        .select("id", "product_id")
    )
)
