from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]))
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 100), 2).alias("quantity_adjusted"),
    )
)
