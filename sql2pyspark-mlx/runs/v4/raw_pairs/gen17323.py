from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2023-11-23") & (F.col("product_id") == 3))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
