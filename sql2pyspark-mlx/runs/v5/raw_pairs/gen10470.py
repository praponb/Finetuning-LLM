from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 210) & (F.col("quantity") != 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("quantity", 0).alias("quantity_rounded"),
        F.round((F.col("unit_price") * 1.15), 2).alias("adjusted_unit_price"),
    )
    .orderBy("id")
)
