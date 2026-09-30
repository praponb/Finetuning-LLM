from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 4) & (F.col("store_id") == 6) & F.col("product_id").isin([4, 8]))
    .select(
        "id",
        "store_id",
        F.round((F.col("quantity") * 100), 2).alias("quantity_scaled"),
    )
)
