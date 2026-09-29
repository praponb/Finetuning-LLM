from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 370) & ((F.col("unit_price") > 350) | F.col("product_id").isNotNull()))
    .select(
        "id",
        "product_id",
        "quantity",
        F.round("quantity", 1).alias("quantity_rounded"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)
