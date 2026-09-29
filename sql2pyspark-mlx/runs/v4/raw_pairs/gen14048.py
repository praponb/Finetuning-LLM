from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]) & (F.col("quantity") <= 204))
    .select(
        "id",
        "product_id",
        "store_id",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)
