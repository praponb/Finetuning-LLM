from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 7) & (F.col("order_id").isin([4, 8]) | F.col("order_id").isin([14, 28, 36])))
    .select(
        "id",
        "order_id",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
