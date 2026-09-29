from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(4, 5))
    .select(
        "order_id",
        "quantity",
        "product_id",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)
