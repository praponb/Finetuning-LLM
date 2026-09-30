from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([8, 9]) | F.col("store_id").isin([1, 2, 3]))
    .select(
        "id",
        "quantity",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
    .orderBy(F.col("last_restock_plus_months").desc())
)
