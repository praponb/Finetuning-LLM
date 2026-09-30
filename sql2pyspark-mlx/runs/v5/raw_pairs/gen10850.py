from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select(
        "last_restock",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
