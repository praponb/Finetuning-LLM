from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.date_sub("last_restock", 66).alias("last_restock_minus_66d"),
    )
    .orderBy(F.col("last_restock_minus_66d").desc())
)
