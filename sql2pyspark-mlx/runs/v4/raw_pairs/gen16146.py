from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2023-08-31") & F.col("store_id").isin([2, 5]))
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_scaled"),
    )
)
