from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "store_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.month("last_restock").alias("last_restock_month"),
    )
    .orderBy(F.col("last_restock_month").desc(), "quantity")
)
