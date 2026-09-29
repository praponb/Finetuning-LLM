from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 82)
    .select(
        "last_restock",
        "store_id",
        "quantity",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
    )
)
