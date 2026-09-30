from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("unit_price") <= 240) & F.col("order_id").isin([4, 18, 39])))
    .select(
        "unit_price",
        "product_id",
        "quantity",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)
