from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6, 8, 9]))
    .select(
        "id",
        "quantity",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
