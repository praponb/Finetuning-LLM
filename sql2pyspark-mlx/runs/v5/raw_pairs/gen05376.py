from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 11, 12]))
    .select(
        "id",
        "last_restock",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
