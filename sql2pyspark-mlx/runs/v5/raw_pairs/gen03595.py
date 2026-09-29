from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 6) & (F.col("unit_price") > 270) & (F.col("unit_price") >= 360))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
    .orderBy(F.col("quantity_rounded").desc(), "id")
    .limit(20)
)
