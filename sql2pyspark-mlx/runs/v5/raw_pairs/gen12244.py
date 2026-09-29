from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .select(
        "id",
        "product_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("product_id").desc())
)
