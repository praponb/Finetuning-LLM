from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 155)
    .select(
        "last_restock",
        "store_id",
        "product_id",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)
