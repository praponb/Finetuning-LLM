from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 7) & (F.col("product_id") == 7) & (F.col("unit_price") > 260))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round("unit_price", 2).alias("unit_price_rounded"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
    .orderBy("id")
)
