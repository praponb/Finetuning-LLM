from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 290)
    .select(
        "id",
        "order_id",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)
