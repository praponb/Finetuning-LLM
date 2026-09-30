from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([10, 12, 17, 24]))
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)
