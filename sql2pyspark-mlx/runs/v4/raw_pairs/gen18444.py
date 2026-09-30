from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 220)
    .select(
        "unit_price",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_filled").desc())
)
