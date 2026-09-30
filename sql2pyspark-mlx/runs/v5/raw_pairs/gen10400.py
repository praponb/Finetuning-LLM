from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 6]) & F.month("last_restock").isin([7, 8, 11]) & (F.col("quantity") < 81))
    .select(
        "last_restock",
        "store_id",
        "quantity",
        F.date_sub("last_restock", 19).alias("last_restock_minus_19d"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)
