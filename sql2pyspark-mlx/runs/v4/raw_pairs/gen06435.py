from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-11-01")
    .select(
        "id",
        "store_id",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
    )
)
