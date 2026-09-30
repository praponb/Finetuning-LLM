from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([18, 30, 32]))
    .select(
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
