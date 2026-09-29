from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 5)
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_adjusted").desc())
)
