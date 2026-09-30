from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([2, 20, 34]) | F.col("product_id").isin([21, 25, 29])))
    .select(
        "id",
        "unit_price",
        "order_id",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)
