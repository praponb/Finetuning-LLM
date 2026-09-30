from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 260)
    .select(
        "id",
        "order_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
